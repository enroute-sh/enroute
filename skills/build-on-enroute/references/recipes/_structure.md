# How a recipe is built

Every file in this directory follows the structure below. It is not a house
style. It is what lets one file serve two readers: an agent that loads it into
a context window, and a person who lands on it from a search engine.

This file has an underscore prefix, so it is not a recipe, and a site does not
publish it.

## The two readers

**An agent** opens one recipe, alone, after it reads `../recipes.md`. Within
a few lines it must learn whether this is the right file, which calls it is
about to make, and what it will get wrong. It reads raw Markdown, so it cannot
see anything the rendering adds.

**A person** may arrive at one recipe having read nothing else. They know the
name Enroute only in part, they never saw the index, and nobody told them the
shared rules the index holds. They scan headings before they read prose, and
they follow links instead of reading in order.

Neither reader reads the directory. A recipe that makes sense only after its
siblings is a recipe with a missing paragraph.

## The problem comes first

A recipe that opens with its answer helps only somebody who already knows they
want it. Every page states the problem before the solution, under headings
that are the same on every recipe, so a reader can tell in one screen whether
this is their problem and why it is not free.

## The spine

1. **`# Title`**: a noun phrase that names the pattern, not a sentence. It is
   the page title on the web and the first line an agent sees.

2. **The lede**: one or two sentences, no heading. It says what the pattern
   is, and not yet how it works. A search engine shows this sentence.

3. **`## The problem`**: this exact heading, on every recipe. Three parts, in
   this order:

   - **The goal**, in the reader's terms and not the contract's.
   - **The gap**: what Enroute does not do on purpose, so the reader sees the
     work left to the application.
   - **The cost of the obvious answer**: what breaks if you do the first thing
     that comes to mind, and how you find out. A failure that looks like
     success is the most valuable sentence on the page.

   Write no part of the solution here. To name the call that solves it is
   already the solution.

4. **`## The solution`**: the core code, with at most one sentence before it.
   Give the smallest correct version, never the full-featured one. The
   sections below add what it leaves out.

5. **The rules**: bold-lead paragraphs. See below.

6. **The sections**: `## ` headings for the stages and components the solution
   left out. Optional. Order them so each one needs only what came before it.

7. **`## What it does not do`**: the limits. Optional, and worth writing
   wherever a reader could expect more than the pattern gives.

8. **`## See also`**: a closing list of links to the sibling recipes, and to
   `../recipes.md` for the rules this one takes as read. Required. Keep the
   inline links where they are; this list is the navigation a web page needs.

Parts 1 to 4 and part 8 are mandatory. A recipe with nothing to put in parts
6 and 7 is a short recipe, which is good.

## The calls name themselves

Do not put a list of calls at the top of a recipe, and do not put a badge
there. The solution names every one of them, in context and in order, with
its arguments around it. `preReceive(req: PreReceiveRequest)` and the
`req.commands` loop under it say more than an index entry could.

The table in `../recipes.md` is what a reader chooses between recipes with,
and it carries that column. A reader inside a recipe has chosen.

This rule protects `checks.md`, which drives no Enroute call at all. Its
opening says so in a sentence.

## Name a shared rule where it applies

The index holds the rules every recipe assumes, and a person who arrived from
a search engine has not read them. A prerequisite link at the top of a page is
rarely followed.

Where a shared rule carries weight in this recipe, state it in the clause
where it applies and link the index there. A push refused per ref, an absent
id spelled one way: say which one applies here, once, at the point it applies.
If you restate the whole rule, the copies drift apart. If you name the one
that carries the weight, the page stands alone.

`../recipes.md` is then in `## See also` as well, for the reader who wants the
whole set.

## Two headings are literal, the rest are claims

Spell `## The problem` and `## The solution` the same way on every recipe. A
reader navigates by these two.

Every other heading is a claim. "Mirror the state, not the event" and
"Enqueue, never build" tell a reader what to do. "About the queue" only tells
them where they are.

## Rules are paragraphs; sections are components

Two devices carry everything below the solution.

**A bold-lead paragraph** states one rule and why it holds.

> **A delete is not a force.** `force` is only ever true for a command that
> has both ids, so a delete arrives with `force` false and no `newObjectId`.

**An `## ` section** covers a distinct stage or component, and carries its own
code block or list.

Make a rule into a heading only when it gains a code block of its own. A
heading with no code and no list is a bold-lead paragraph that grew too long.

## Conventions

**Code is TypeScript, and the fence says so.** One language.
`references/contract.md` names its toolchain first, and a reader who
translates real code has an easier task than one who translates a dialect
nobody runs. It is a reference, not a restriction. The skill builds in the
language the user asked for.

**Write field names as a generator writes them.** Every TypeScript plugin
lowerCamelCases the proto, so it is `oldObjectId`, `commitId`, and `repoPath`.
Enum values lose the prefix their proto name carries, which gives
`Denial.NOT_FOUND` and `Access.WRITE`. Write the long nested ones through an
import alias, not inline.

**A repository is a `RepoKey`, and the key is the application's own id.**
`{ key: row.id }` going out, `req.repo.key` coming back. Never a name.

**An id is an `ObjectId`, never a bare string.** `{ hex: tip }` going out and
`ref.objectId!.hex` coming back. An absent id is an unset field, so the test
is `!c.newObjectId`, never a comparison against forty zeroes or `""`.

**Write message literals plainly.** `ts-proto` takes them as they are.
`protobuf-es` needs `create(Schema, { … })` around each one, which would bury
the decision the example shows. The recipes leave it out, and this line says
so.

**A oneof keeps its wrapper.** Write `{ case: "denied", value: { … } }` out
in full. A hook that answers the wrong shape answers nothing, and this is a
bug the ceremony prevents. A recipe with several such returns gives itself a
two-line helper, as an application would.

**Write a streaming RPC as a loop.** `GetObject`, `ListTree`, `ListCommits`,
and `DiffCommit` return a stream. Consume each one with `for await` and fold
its `truncated` flag across the pages.

**Links are relative and end in `.md`.** GitHub follows them, a site generator
rewrites them, and an agent reading the raw file can open them.

**The filename is the URL.** Name a file for the pattern a person would search
for.

**The limit is about 150 lines.** A recipe that wants more is usually two
recipes, or one recipe and a rule that belongs in `../recipes.md`.

**A rule that appears in three recipes belongs in the index.** The shared
rules exist so a recipe can assume them.

**Write in ASD-STE100 Simplified Technical English.** Short sentences, active
voice, one idea per sentence, no metaphor.

## Before committing a recipe

- The lede says what the pattern is, and nothing about how.
- `## The problem` names a failure, not only a gap.
- No call is named before `## The solution`.
- Every shared rule that carries weight here is named where it applies.
- Every claim about the contract is checked against `proto/enroute/`, not
  remembered.
- No heading carries a rule that has no code under it.
- The prose is Simplified Technical English.
- The file reads correctly to somebody who has read nothing else.
