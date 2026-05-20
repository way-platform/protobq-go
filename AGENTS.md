# AGENTS.md

## Structure

- The project uses [Buf](https://buf.build) to manage protobuf schemas in [proto](./proto).

- Build tasks are declared in [mise.toml](./mise.toml) and run with `mise run <task>`.

## Developing

- Run `mise install` to install all required tools.

- After making changes, run the full build with `mise run build` and fix any issues.

- Leave all version control and git to the user/developer. If you see a build error
  related to having a git diff, this is normal.
