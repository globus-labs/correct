# CORRECT

[![GitHub Super-Linter](https://github.com/globus-labs/correct/actions/workflows/linter.yml/badge.svg)](https://github.com/globus-labs/correct/actions/workflows/linter.yml)
[![CI](https://github.com/globus-labs/correct/actions/workflows/ci.yml/badge.svg)](https://github.com/globus-labs/correct/actions/workflows/ci.yml)
[![Check dist/](https://github.com/globus-labs/correct/actions/workflows/check-dist.yml/badge.svg)](https://github.com/globus-labs/correct/actions/workflows/check-dist.yml)
[![CodeQL](https://github.com/globus-labs/correct/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/globus-labs/correct/actions/workflows/codeql-analysis.yml)
[![Coverage](./badges/coverage.svg)](./badges/coverage.svg)

> [!WARNING]
>
> Action is under active development.

## Introduction

This COntinuous Reproducibility with a Remote Execution Tool (CORRECT) action
aims to allow users to execute their jobs (and by extension, CI flows) on
[Globus Compute](https://globus-compute.readthedocs.io/en/stable/sdk.html)
Endpoints within their GitHub Action workflows.

Users are responsible for setting up their own Globus Compute Endpoints that
will be used by the action. Furthermore, to bypass manual authentication within
the action, it is necessary that the endpoints and the action are both
configured to use a `GLOBUS_COMPUTE_CLIENT_ID` and
`GLOBUS_COMPUTE_CLIENT_SECRET`
([docs](https://globus-compute.readthedocs.io/en/stable/sdk.html#client-credentials-with-clients)).

## Usage

Using the action in a workflow can be done by adding the following step.

With a pre-registered function

```yaml
- name: Run task on Globus Compute Endpoint
  id: gc-action
  uses: globus-labs/correct@v1
  with:
    client_id: ${{ secret.GLOBUS_COMPUTE_CLIENT_ID }}
    client_secret: ${{ secret.GLOBUS_COMPUTE_CLIENT_SECRET }}
    endpoint_uuid: <Globus Compute Endpoint UUID>
    function_uuid: <Registered Globus Compute function UUID>
    args: <List of arguments to pass to task>
    kwargs: <Dictionary of keyword arguments>
```

With a shell command

```yaml
- name: Run task on Globus Compute Endpoint
  id: gc-action
  uses: globus-labs/correct@v1
  with:
    client_id: ${{ secret.GLOBUS_COMPUTE_CLIENT_ID }}
    client_secret: ${{ secret.GLOBUS_COMPUTE_CLIENT_SECRET }}
    endpoint_uuid: <Globus Compute Endpoint UUID>
    shell_cmd: '<shell command to run on remote resources>'
```

### Example

```yaml
- name: Run GC Action
  id: gc-action
  uses: globus-labs/correct@v1
  with:
    client_id: ${{ secrets.GLOBUS_COMPUTE_CLIENT_ID }}
    client_secret: ${{ secrets.GLOBUS_COMPUTE_CLIENT_SECRET }}
    endpoint_uuid: 'f8e95115-0d66-41fe-88d8-ecf8c3bf59fd'
    function_uuid: '02ea7614-be2e-4df0-9d23-643b6d8a6499'
    args: '[]'
    kwargs: '{"inpt": "test"}'
```

```yaml
- name: Run GC Action
  id: gc-action
  uses: globus-labs/correct@v1
  with:
    client_id: ${{ secrets.GLOBUS_COMPUTE_CLIENT_ID }}
    client_secret: ${{ secrets.GLOBUS_COMPUTE_CLIENT_SECRET }}
    endpoint_uuid: 'f8e95115-0d66-41fe-88d8-ecf8c3bf59fd'
    shell_cmd: 'pytest'
```

### Obtaining task execution outputs

The output of the execution of a GC task is returned as a JSON dictionary via
the `steps.<step_id>.outputs.output` variable. For example, a job defined as
follows:

```yaml
- name: Run GC Action
  id: gc-action
  uses: globus-labs/correct@v1
  with:
    client_id: ${{ secrets.GLOBUS_COMPUTE_CLIENT_ID }}
    client_secret: ${{ secrets.GLOBUS_COMPUTE_CLIENT_SECRET }}
    endpoint_uuid: 'f8e95115-0d66-41fe-88d8-ecf8c3bf59fd'
    function_uuid: '02ea7614-be2e-4df0-9d23-643b6d8a6499'
    args: '[]'
    kwargs: '{"inpt": "test"}'
- name: Print Output
  id: output
  run: echo "${{ steps.gc-action.outputs.output }}"
```

May return the following output:

```bash
{
  task_id: '35363d35-a3a9-4db7-bc46-4a8d92941188',
  status: 'success',
  result: '00\ngASVEwAAAAAAAAB9lIwEaW5wdJSMBXdvcmxklHMu\n',
  completion_t: '1738514959.424744',
  details: {
    os: 'macOS-15.0.1-arm64-arm-64bit',
    dill_version: '0.3.9',
    python_version: '3.12.0',
    globus_compute_sdk_version: '3.0.1',
    endpoint_id: 'f8e95115-0d66-41fe-88d8-ecf8c3bf59fd',
    task_transitions: {
      'execution-start': 1738514959.122622,
      'execution-end': 1738514959.14045
    }
  }
}
```
