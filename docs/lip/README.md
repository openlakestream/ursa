# Lakestream Improvement Proposals (LIPs)

A LIP is a short design document for a change that other people build on: the public API, an SPI
contract, a storage format, or a new materializer. Writing the design down before the code lets
reviewers and future implementers agree on *what* is changing, and why, before anyone debates *how*.

## When you need a LIP

You need a LIP for:

- new or changed public types in `lakestream-api`
- changes to an SPI contract, such as `TableMaterializer` or `TableMaterializerFactory`
- changes to the on-object or WAL formats, or to serialized field numbers and identifiers
- every new materializer, because registering one adds a `TableCatalogType` to `lakestream-api`
  (see [Write a materializer](../developer/materializer-guide.md))

You don't need a LIP for bug fixes, internal refactoring, performance work that keeps behavior and
formats unchanged, tests, or documentation. If you're not sure, ask in
[Discussions: Ideas](https://github.com/lakestream-io/ursa/discussions/categories/ideas).

## How it works

1. **Start a discussion.** Open a thread in
   [Discussions: Ideas](https://github.com/lakestream-io/ursa/discussions/categories/ideas).
   Describe the problem: what you're trying to build, and what gets in the way today. The first
   step is agreeing that the problem is worth solving. For a new materializer, open a
   [New materializer proposal](https://github.com/lakestream-io/ursa/issues/new?template=materializer_proposal.yml)
   issue instead of a discussion.
2. **Write the LIP.** Copy [TEMPLATE.md](TEMPLATE.md) to `docs/lip/LIP-NNN-Short-Title.md`.
   Use the highest existing LIP number plus one. If two open pull requests pick the same number,
   the one merged second renumbers.
3. **Open a pull request** that adds the file with the status *Proposed*, and link it from the
   discussion. Keep the design review on the pull request, so the document and its review stay
   together.
4. **Review.** The code owners for `docs/lip/` and for the modules the LIP affects review it. Expect
   questions about compatibility, rollback, alternatives and testing.
5. **Merge.** Merging the pull request accepts the LIP. Implementation pull requests link to it.
   A LIP that isn't accepted is closed, with the reasons recorded on the pull request.
6. **Keep the status current** as the work lands.

## Status

| Status | Meaning |
|---|---|
| Proposed | Under review in a pull request |
| Accepted | Merged; implementation can start |
| Implemented | The code has landed on `main` |
| Released | Shipped in a release; record the version |
| Superseded | Replaced by a later LIP; link to it |

## Writing a good LIP

- Lead with the problem. A reader should understand why the change matters before reading the
  design.
- Be explicit about compatibility. Say what happens on upgrade and on rollback, and call breaking
  changes breaking.
- Keep it as short as the change allows. Link to existing documentation instead of repeating it.
- Record the alternatives you rejected, and why. They stop the same debate from happening twice.

## Index

| LIP | Title | Status |
|---|---|---|
| [161](LIP-161-Table-Materialization-Framework.md) | Table Materialization Framework | Released in 1.0.0 |
