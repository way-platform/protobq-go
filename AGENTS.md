# AGENTS.md

## Structure

- The project uses a [tools](./tools/) directory with a separate Go module containing tools for building, linting and generating code.

- The project uses [Buf](https://buf.build) to manage protobuf schemas in [proto](./proto).

- The project uses [Mage](https://magefile.org) with build tasks declared in [magefile.go](./tools/magefile.go).

## Developing

- After making changes, run the default build task with `./tools/mage` and fix any issues.

- Leave all version control and git to the user/developer. If you see a build error related to having a git diff, this is normal.
