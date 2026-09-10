//! Instructions retired, where the hardware will count them.
//!
//! Time — wall or CPU — measures the machine as much as the code: a second
//! copy of this bench alongside the first moved identical work from 34s of
//! CPU to 52s. Instructions retired is the count of work done, and doesn't
//! move when the clock does. [`Sample::system`] is the number to optimise:
//! it's the only one that can see Postgres, so a change trading local work
//! for more queries can't read as a free win. System-wide counting is only
//! meaningful on an otherwise-idle box — one sprite per experiment, never
//! shared.

/// One reading of both counters.
#[derive(Debug, Default, Clone, Copy, serde::Serialize)]
pub(crate) struct Sample {
    /// `None` where the platform has no counter to ask.
    pub(crate) process: Option<u64>,
    /// `None` when `perf_event_paranoid` forbids system-wide observation.
    pub(crate) system: Option<u64>,
}

impl Sample {
    /// What was retired between `earlier` and `self`.
    pub(crate) fn since(self, earlier: Self) -> Self {
        /// Counters only climb, so a wrapped subtraction means a lost or
        /// reset counter rather than negative work.
        fn delta(now: Option<u64>, then: Option<u64>) -> Option<u64> {
            now?.checked_sub(then?)
        }
        Self {
            process: delta(self.process, earlier.process),
            system: delta(self.system, earlier.system),
        }
    }
}

#[cfg(target_os = "linux")]
pub(crate) use linux::Counters;

#[cfg(target_os = "linux")]
mod linux {
    use std::fmt;

    use perf_event::events::Hardware;
    use perf_event::{Builder, Counter};

    use super::Sample;

    /// Live counters.
    ///
    /// Build before anything that should be counted exists: an inherited
    /// counter follows threads started *after* it, missing a pool created first.
    pub(crate) struct Counters {
        process: Option<Counter>,
        /// One per CPU — a system-wide counter observes a single core.
        system: Vec<Counter>,
    }

    impl fmt::Debug for Counters {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("Counters")
                .field("process", &self.process.is_some())
                .field("system_cpus", &self.system.len())
                .finish()
        }
    }

    fn process_counter() -> std::io::Result<Counter> {
        // `inherit` borrows where the rest of the builder consumes, so it
        // can't join the chain.
        let mut builder = Builder::new()
            .kind(Hardware::INSTRUCTIONS)
            .observe_self()
            .any_cpu();
        builder.inherit(true);
        let mut counter = builder.build()?;
        counter.enable()?;
        Ok(counter)
    }

    fn system_counter(cpu: usize) -> std::io::Result<Counter> {
        let mut counter = Builder::new()
            .kind(Hardware::INSTRUCTIONS)
            .any_pid()
            .one_cpu(cpu)
            .build()?;
        counter.enable()?;
        Ok(counter)
    }

    /// All or nothing: a partial set would silently undercount by whatever ran
    /// on the cores that had no counter.
    fn system_counters(cpus: usize) -> Result<Vec<Counter>, String> {
        let mut counters = Vec::with_capacity(cpus);
        for cpu in 0..cpus {
            counters.push(system_counter(cpu).map_err(|e| {
                format!(
                    "no system-wide counter on cpu {cpu} ({e}); Postgres will \
                     not be counted — needs kernel.perf_event_paranoid <= 0"
                )
            })?);
        }
        Ok(counters)
    }

    impl Counters {
        /// Missing counters degrade to `None` rather than failing, and say so
        /// in the returned notes — an empty column must never read as a zero.
        pub(crate) fn start() -> (Self, Vec<String>) {
            let mut notes = Vec::new();

            let process = match process_counter() {
                Ok(c) => Some(c),
                Err(e) => {
                    notes.push(format!("no per-process counter: {e}"));
                    None
                }
            };

            let cpus = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
            let system = system_counters(cpus).unwrap_or_else(|note| {
                notes.push(note);
                Vec::new()
            });

            (Self { process, system }, notes)
        }

        pub(crate) fn sample(&mut self) -> Sample {
            Sample {
                process: self.process.as_mut().and_then(|c| c.read().ok()),
                system: self
                    .system
                    .iter_mut()
                    .map(|c| c.read().ok())
                    .sum::<Option<u64>>(),
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub(crate) use portable::Counters;

#[cfg(not(target_os = "linux"))]
mod portable {
    use super::Sample;

    /// Stands in where there are no hardware counters to read.
    ///
    /// macOS has no `perf_event_open`, nor a guest under Apple's
    /// Virtualization.framework, so a local run reports time only.
    #[derive(Debug)]
    pub(crate) struct Counters;

    impl Counters {
        pub(crate) fn start() -> (Self, Vec<String>) {
            (
                Self,
                vec!["no instruction counters on this platform; run in a sprite".to_owned()],
            )
        }

        #[expect(
            clippy::unused_self,
            reason = "matches the Linux `Counters::sample`, which this stands in for"
        )]
        pub(crate) fn sample(&mut self) -> Sample {
            Sample::default()
        }
    }
}
