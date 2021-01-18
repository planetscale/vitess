---
title: Coding Standards
---

## Backwards Compatibility

Vitess is being used to power many mission-critical production workloads at very large scale. 
Moreover, many users deploy directly from the master branch. 
It is very important the changes made by contributors do not break any existing workloads.

In order to avoid disruption, the following concerns need to be kept in mind:
* Does the change affect any external APIs? If so, make sure the change satisfies the [compatibility rules](https://github.com/vitessio/enhancements/blob/master/veps/vep-3.md).
* Can the change introduce a performance regression? If so, it will be good to measure the impact using benchmarks.
* If the change is substantial or is a breaking change, you must publish the proposal as an issue with a title like `RFC: Changing behavior of feature xxx`. Following this, sufficient time has to be given for others to give feedback. A breaking change must still satisfy the compatibility rules.
* New features that affect existing behavior must be introduced "behind a flag". Users will then be encouraged to enable them, but will have the option to fallback to the old behavior if issues are found.

## What does a good PR look like?

Every GitHub pull request must go through a code review and get approved before it will be merged into the master branch.

Every pull request should meet the following requirements:
* Use the [Pull Request Template](https://github.com/vitessio/vitess/blob/master/.github/PULL_REQUEST_TEMPLATE/pull_request_template.md) 
* Adhere to the [Go coding guidelines](https://golang.org/doc/effective_go.html) and watch out for these [common errors](https://github.com/golang/go/wiki/CodeReviewComments).
* Contain a description message that is as detailed as possible. Here is a great example https://github.com/vitessio/vitess/pull/6543.
* Pass all CI tests that run on PRs.
* For bigger changes, it is a good idea to start by creating an RFC (Request for Commnent) issue - this is where you can discuss the feature and why it's important.
Once that is in place, you can create the PR, as a solution to the problem described in the issue. Separating the need and the solution this way makes discussions easier and more focused.

### Testing

We use unit tests both to test the code and to describe it for other developers. 

* Unit tests should:
  * Demonstrate every use case the change covers.
  * Involve all important units being added or changed.
  * Attempt to cover every corner case the change introduces. 
  The thumb rule is: if it can happen in production, it must be covered.
* Integration tests should ensure that the feature works end-to-end. 
They must cover all the important use cases of the feature.
* A separate pull request into `vitessio/website` that updates the documentation is required if the feature changes or adds to existing behavior.

### Bug fixes

If you are creating a PR to fix a bug, make sure to create an end-to-end test that fails without your change.
This is the important reproduction case that will make sure this particular bug does not show up again, and that clearly shows on your PR what bug you are fixing.

While you are fixing the bug, it's valuable if you take the time to step back and try to think of other places where this problem could have impacted. 
It's often possible to infer that other similar problems in this or other parts of the code base can be prevented.

Some additional points to keep in mind:
*   Does this change match an existing design / bug?
*   Is this change going to log too much? (Error logs should only happen when
    the component is in bad shape, not because of bad transient state or bad
    user queries)
*   Does this match our current patterns? Example include RPC patterns,
    Retries / Waits / Timeouts patterns using Context, ...

We also recommend that every author look over their code change before committing and ensure that the recommendations below are being followed. This can be done by skimming through `git diff --cached` just before committing.

*   Scan the diffs as if you're the reviewer.
    *   Look for files that shouldn't be checked in (temporary/generated files).
    *   Look for temporary code/comments you added while debugging.
       *   Example: `fmt.Println("AAAAAAAAAAAAAAAAAA")`
    *   Look for inconsistencies in indentation.
       *   Use 2 spaces in everything except Go.
       *   In Go, just use goimports.
*   Commit message format:
    *   ```
        <component>: This is a short description of the change.

        If necessary, more sentences follow e.g. to explain the intent of the change, how it fits into the bigger picture or which implications it has (e.g. other parts in the system have to be adapted.)

        Sometimes this message can also contain more material for reference e.g. benchmark numbers to justify why the change was implemented in this way.
        ```
*   Comments
    *   `// Prefer complete sentences when possible.`
    *   Leave a space after the comment marker `//`.

If your reviewer leaves comments, make sure that you address them and then click "Resolve conversation". There should be zero unresolved discussions when the PR merges.

## Assigning a Pull Request

Vitess uses [code owners](https://github.blog/2017-07-06-introducing-code-owners/) to auto-assign reviewers to a particular PR. If you have been granted membership to the Vitess team, you can add additional reviewers using the right-hand side pull request menu.

During discussions, you can also refer to somebody using the *@username* syntax and they'll receive an email as well.

If you want to receive notifications even when you aren't mentioned, you can go to the [repository page](https://github.com/vitessio/vitess) and click *Watch*.

## Approving a Pull Request

As a reviewer you can approve a pull request through two ways:

* Approve the pull request via GitHub's code review system
* Reply with a comment that contains *LGTM*  (Looks Good To Me)

## Merging a Pull Request

The Vitess team will merge your pull request after the PR has been approved and CI tests have passed.
