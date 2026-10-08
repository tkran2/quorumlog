# Mac setup and GitHub publishing

The archive contains source code; your Mac builds its own executables.

## Download and build

Download `quorumlog.zip` into your Downloads folder. In Terminal:

```bash
mkdir -p ~/Projects
unzip ~/Downloads/quorumlog.zip -d ~/Projects
cd ~/Projects/quorumlog
c++ --version
make -j2
make test
make demo
```

If the compiler is missing, run `xcode-select --install`, finish the installer,
and repeat `make -j2`. If Python 3 is missing and you have Homebrew, run
`brew install python`. The scripts use only Python's standard library.

`make test` starts real three-node clusters on free local ports and takes about
half a minute. macOS may ask whether to allow incoming network connections for
`quorum-node`; the tests only use the loopback address, so either answer works.

These commands were verified on Linux with GCC 13. The code uses only POSIX
sockets and standard C++20, and the CI workflow builds and tests on macOS, but
the delivered run was not made on a Mac. If something fails there, the first
places to look are the compiler version (`c++ --version` should report Apple
clang 15 or newer) and the benchmark numbers, which will differ because macOS
`fsync` behaves differently from Linux.

## Publish to GitHub from Terminal

Install the GitHub CLI if `gh --version` does not work:

```bash
brew install gh
```

If Homebrew is not installed, use the official instructions at
https://brew.sh/, then install `gh`.

Authenticate using the interactive terminal flow:

```bash
gh auth login --scopes workflow
gh auth setup-git
```

Choose GitHub.com and HTTPS. The flow offers browser sign-in or token sign-in.
The extra scope permits uploading the included GitHub Actions workflow.

Create and publish the repository:

```bash
cd ~/Projects/quorumlog
git init -b main
git add .
git -c user.name='Tom Krane' -c user.email='tkran2@users.noreply.github.com' commit -m 'Build Raft replicated log with deterministic simulation and linearizability checking'
gh repo create quorumlog --public --source=. --remote=origin --push
gh repo view --json url --jq .url
```

The command creates a public repository under the account selected by
`gh auth login`. If `quorumlog` already exists, choose a different unused
name in the `gh repo create` command. It will not overwrite an existing repo.

## Save later changes

```bash
make test
git add .
git commit -m 'Describe the change you made'
git push
```

Only the initial commit above supplies a name and email for that one command.
If your Mac's Git identity is not configured, configure it before later commits:

```bash
git config user.name 'Tom Krane'
git config user.email 'tkran2@users.noreply.github.com'
```

These settings apply to this repository. The noreply email avoids publishing
your school email in Git commit metadata.
