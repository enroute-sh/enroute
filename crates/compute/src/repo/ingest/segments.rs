//! Segments: each new commit's tree diff against its parents, run frame by
//! frame as a [`Machine`](machine::Machine).

use std::sync::Arc;

use anyhow::{Context, Result};
use foldhash::{HashMap, HashMapExt};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::git::oid::Oid;
use crate::repo::machine;
use crate::repo::view::must;
use crate::storage::{BitSet, LayerBuilder, NewCommit, TempTable, Workspace};
use crate::util::{ix, to_u64, to_usize};

use super::diff::{Diffed, Slot, diff_level};
use super::{Assign, Commits, Ctx};

/// One level of a commit's tree diff (see [`Segments`]).
struct Frame {
    tree: Oid,
    /// The parents' versions at this path.
    parents: Vec<Option<Oid>>,
    /// Which commit of the window.
    commit: usize,
    /// Output in order; children's output goes where they are.
    out: Vec<Slot>,
}

/// Deltas of new trees against their assigned bases, by entry, in temp
/// tables that spill to disk.
pub(super) struct TreeDeltas {
    /// Where each entry's delta is in `log` (`len` 0: none).
    at: TempTable<DeltaAt>,
    log: TempTable<u8>,
}

#[derive(Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
struct DeltaAt {
    at: u64,
    len: u32,
    _pad: u32,
}

impl TreeDeltas {
    fn new(n: usize, ws: &Workspace) -> TreeDeltas {
        let none = DeltaAt {
            at: 0,
            len: 0,
            _pad: 0,
        };
        TreeDeltas {
            at: TempTable::filled(ws, n, none),
            log: TempTable::new(ws),
        }
    }

    fn put(&mut self, i: usize, delta: &[u8]) -> Result<()> {
        self.at.set(
            i,
            DeltaAt {
                at: to_u64(self.log.len()),
                len: u32::try_from(delta.len()).context("tree delta too large")?,
                _pad: 0,
            },
        );
        self.log.extend_from_slice(delta);
        Ok(())
    }

    /// `f` of entry `i`'s delta, if it has one.
    pub(super) fn with<R>(&self, i: usize, f: impl FnOnce(&[u8]) -> R) -> Option<R> {
        let r = self.at.get(i);
        let at = to_usize(r.at);
        (r.len > 0).then(|| self.log.with_slice(at..at + ix(r.len), f))
    }
}

const COMMIT_WINDOW: usize = 4096;
/// Frames diffed per parallel work unit.
const FRAME_BLOCK: usize = 64;

/// Segments of the new commits and delta base proposals, as a
/// [`Machine`](machine::Machine): each commit's tree diff against its
/// parents is split into frames (one tree level each). Frames run in
/// parallel; a frame whose trees are not local (a parent's version in the
/// object store) waits for them while the others go on. Once a window's
/// frames are done, their output is merged in topological order and in
/// each commit's depth-first order, exactly as one recursive pass would:
/// the first commit to introduce an object assigns its base.
pub(super) struct Segments {
    cx: Arc<Ctx>,
    commits: Commits,
    order: Vec<u32>,
    /// Next window's start in `order`.
    next: usize,
    window: Vec<u32>,
    frames: Vec<Frame>,
    /// Root frame of each commit of the window (`None`: tree unchanged).
    roots: Vec<Option<usize>>,
    ready: Vec<usize>,
    parked: HashMap<machine::Ticket, usize>,
    /// Pins on what parked frames waited for, until they are diffed.
    pins: HashMap<usize, Vec<crate::storage::Pinned>>,
    /// The result, built window by window.
    out: Option<SegmentsOut>,
}

pub(super) struct SegmentsOut {
    pub(super) builder: LayerBuilder,
    pub(super) assign: TempTable<Assign>,
    pub(super) tree_deltas: TreeDeltas,
    pub(super) member_order: TempTable<u32>,
    pub(super) in_order: BitSet,
}

impl Segments {
    pub(super) fn new(cx: Arc<Ctx>, commits: Commits, order: Vec<u32>, ws: &Workspace) -> Segments {
        let n = cx.idx.len();
        Segments {
            out: Some(SegmentsOut {
                builder: LayerBuilder::new(ws, cx.snap.format()),
                assign: TempTable::filled(
                    ws,
                    n,
                    Assign {
                        base: Oid::ZERO,
                        ctx: Oid::ZERO,
                    },
                ),
                tree_deltas: TreeDeltas::new(n, ws),
                member_order: TempTable::new(ws),
                in_order: BitSet::new(n),
            }),
            cx,
            commits,
            order,
            next: 0,
            window: Vec::new(),
            frames: Vec::new(),
            roots: Vec::new(),
            ready: Vec::new(),
            parked: HashMap::new(),
            pins: HashMap::new(),
        }
    }

    /// Start the next window's diffs.
    fn start_window(&mut self) -> Result<()> {
        let end = (self.next + COMMIT_WINDOW).min(self.order.len());
        self.window = must(self.order.get(self.next..end), "commit window in range").to_vec();
        self.next = end;
        self.frames.clear();
        self.roots.clear();
        for (w, &c) in self.window.iter().enumerate() {
            let info = self.commits.rec(ix(c));
            let parents = self.commits.parents(&info);
            let parent_trees = parents
                .iter()
                .map(|p| self.cx.commit_tree(&self.commits, p).map(Some))
                .collect::<Result<Vec<_>>>()?;
            if parent_trees.contains(&Some(info.tree)) {
                self.roots.push(None);
                continue;
            }
            self.roots.push(Some(self.frames.len()));
            self.ready.push(self.frames.len());
            self.frames.push(Frame {
                tree: info.tree,
                parents: parent_trees,
                commit: w,
                out: Vec::new(),
            });
        }
        Ok(())
    }

    /// Merge a finished window: commits in topological order, each in
    /// depth-first order.
    fn finish_window(&mut self) -> Result<()> {
        let out = must(
            self.out.as_mut(),
            "window merged before the result is taken",
        );
        let mut proposals: Vec<(usize, Assign, Option<Vec<u8>>)> = Vec::new();
        for (w, &c) in self.window.iter().enumerate() {
            let info = self.commits.rec(ix(c));
            let mut members = Vec::new();
            let root = *must(self.roots.get(w), "a root frame entry per commit");
            if let Some(root) = root {
                collect_members(
                    &mut self.frames,
                    &self.cx,
                    root,
                    &mut members,
                    &mut proposals,
                )?;
            }
            let ci = self.cx.new_entry(&info.oid).context("commit not new")?;
            out.push_member(ci)?;
            for i in members.iter().filter_map(|m| self.cx.new_entry(m)) {
                out.push_member(i)?;
            }
            out.builder.add_commit(&NewCommit {
                oid: info.oid,
                tree: info.tree,
                parents: self.commits.parents(&info),
                time: info.time,
                members,
            })?;
        }
        // Applied in index order (each page of `assign` written once per
        // window); stable, so the first proposal per object wins.
        proposals.sort_by_key(|(i, _, _)| *i);
        let mut last = usize::MAX;
        for (i, a, d) in proposals {
            if i != last && out.assign.get(i).base.is_zero() {
                out.assign.set(i, a);
                d.map(|d| out.tree_deltas.put(i, &d)).transpose()?;
            }
            last = i;
        }
        Ok(())
    }

    /// Move on through the windows until there are frames to diff, all
    /// frames wait, or there is no next window.
    fn settle(&mut self) -> Result<Settled> {
        while self.ready.is_empty() && self.parked.is_empty() {
            if let Some(done) = self.next_window()? {
                return Ok(Settled::Done(Box::new(done)));
            }
        }
        Ok(if self.ready.is_empty() {
            Settled::Waiting
        } else {
            Settled::Ready
        })
    }

    /// Merge the finished window, if any, and start the next; the result if
    /// there is no next.
    fn next_window(&mut self) -> Result<Option<SegmentsOut>> {
        if !self.window.is_empty() {
            self.finish_window()?;
            self.window.clear();
        }
        if self.next == self.order.len() {
            return Ok(Some(must(self.out.take(), "result taken once")));
        }
        self.start_window()?;
        Ok(None)
    }

    /// Diff the frames `batch` in parallel blocks.
    fn diff_batch(&self, batch: &[usize]) -> Vec<Result<Vec<Diffed>>> {
        let (cx, frames, window, commits) = (&self.cx, &self.frames, &self.window, &self.commits);
        crate::util::par_blocks(batch.len().div_ceil(FRAME_BLOCK), |b| {
            batch
                .chunks(FRAME_BLOCK)
                .nth(b)
                .unwrap_or_default()
                .iter()
                .map(|&f| diff_frame(cx, frames, window, commits, f))
                .collect()
        })
    }

    /// Take the result of diffing frame `f`: park it if it waits for bytes,
    /// else queue the frames of its subtrees.
    fn apply_diffed(&mut self, f: usize, d: Diffed, waits: &mut machine::Waits) {
        match d {
            Diffed::Missing(need) => {
                let t = waits.park(need);
                self.parked.insert(t, f);
            }
            Diffed::Done(children, out) => self.finish_frame(f, children, out),
        }
    }

    /// Set the output `out` of frame `f`, and queue a frame for each of its
    /// child slots.
    fn finish_frame(
        &mut self,
        f: usize,
        children: Vec<(Oid, Vec<Option<Oid>>)>,
        mut out: Vec<Slot>,
    ) {
        let commit = must(self.frames.get(f), "frame in range").commit;
        let mut kids = children.into_iter();
        for c in child_slots(&mut out) {
            let (tree, parents) = must(kids.next(), "one child per slot");
            *c = self.frames.len();
            self.ready.push(*c);
            self.frames.push(Frame {
                tree,
                parents,
                commit,
                out: Vec::new(),
            });
        }
        must(self.frames.get_mut(f), "frame in range").out = out;
    }
}

/// The frame indexes of the child slots in `out`.
fn child_slots(out: &mut [Slot]) -> impl Iterator<Item = &mut usize> {
    out.iter_mut().filter_map(|s| match s {
        Slot::Child(c) => Some(c),
        _ => None,
    })
}

impl SegmentsOut {
    /// Append pack entry `i` to the member order, once.
    fn push_member(&mut self, i: usize) -> Result<()> {
        if self.in_order.insert(i) {
            self.member_order
                .push(u32::try_from(i).context("pack has too many objects")?);
        }
        Ok(())
    }
}

/// Walk the output of the frame tree under `root` depth-first: the members
/// go to `members`, the delta base proposals to `proposals`.
fn collect_members(
    frames: &mut [Frame],
    cx: &Ctx,
    root: usize,
    members: &mut Vec<Oid>,
    proposals: &mut Vec<(usize, Assign, Option<Vec<u8>>)>,
) -> Result<()> {
    let mut stack = vec![(root, 0usize)];
    while let Some((f, k)) = stack.pop() {
        let frame = must(frames.get_mut(f), "frame in range");
        let Some(slot) = frame.out.get_mut(k) else {
            continue;
        };
        stack.push((f, k + 1));
        match slot {
            Slot::Member(o) => members.push(*o),
            Slot::Assign(o, a, d) => {
                let i = cx.new_entry(o).context("assigned object not new")?;
                proposals.push((i, *a, d.take()));
            }
            Slot::Child(child) => stack.push((*child, 0)),
        }
    }
    Ok(())
}

/// Diff frame `f` of the current window.
fn diff_frame(
    cx: &Ctx,
    frames: &[Frame],
    window: &[u32],
    commits: &Commits,
    f: usize,
) -> Result<Diffed> {
    let fr = must(frames.get(f), "frame in range");
    let c = *must(window.get(fr.commit), "commit inside the window");
    let parent_commits = commits.parents(&commits.rec(ix(c)));
    diff_level(cx, fr.tree, &fr.parents, &parent_commits)
}

/// Where [`Segments::settle`] leaves the work.
enum Settled {
    /// Every window is merged.
    Done(Box<SegmentsOut>),
    /// Every frame at hand waits for bytes.
    Waiting,
    /// There are frames to diff.
    Ready,
}

impl machine::Machine for Segments {
    type Output = SegmentsOut;

    fn run(&mut self, waits: &mut machine::Waits) -> Result<Option<SegmentsOut>> {
        loop {
            match self.settle()? {
                Settled::Done(out) => return Ok(Some(*out)),
                Settled::Waiting => return Ok(None),
                Settled::Ready => {}
            }
            let batch = std::mem::take(&mut self.ready);
            let results = self.diff_batch(&batch);
            for f in &batch {
                self.pins.remove(f);
            }
            let mut diffed = Vec::with_capacity(batch.len());
            for block in results {
                diffed.extend(block?);
            }
            for (&f, d) in batch.iter().zip(diffed) {
                self.apply_diffed(f, d, waits);
            }
        }
    }

    fn resume(&mut self, ticket: machine::Ticket, pins: Vec<crate::storage::Pinned>) {
        if let Some(f) = self.parked.remove(&ticket) {
            self.ready.push(f);
            self.pins.insert(f, pins);
        }
    }
}
