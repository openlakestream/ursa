# Security policy

## Supported versions

We fix security issues in the latest release line (currently 1.0.x). Fixes land on `main` and ship in
the next release. Older releases don't receive patches, so upgrade to the latest release to pick up a
fix.

## Reporting a vulnerability

**Please don't report security problems in a public issue, pull request or discussion.**

Report them privately, in either of these ways:

1. **GitHub private vulnerability reporting.** Use the *Report a vulnerability* button on the
   [Security tab](https://github.com/lakestream-io/ursa/security) of this repository. We prefer this
   channel: it's private, it keeps the conversation in one thread, and it stays attached to the
   repository.
2. **Email `security@streamnative.io`**, with the repository name in the subject line.

As much as you have of the following helps us act quickly:

- the affected version or commit
- what an attacker could do, and under which configuration
- steps to reproduce the problem
- anything you already know about the impact

A rough report sent early is better than a polished one sent late.

## What happens next

We'll acknowledge your report, investigate it, and keep you updated as we go. We coordinate
disclosure with you. By default we aim to publish within 90 days of the report, and sooner once a fix
is available.

When the fix is released, we publish a security advisory in this repository and credit you in it,
unless you ask us not to.

We don't run a bug bounty program.

## Scope

This policy covers the code in this repository. Vulnerabilities in StreamNative's commercial
products or hosted services can also be reported to `security@streamnative.io`, and we'll route them
to the right team.
