/**
 * Unit tests for the action's main functionality, src/main.ts
 *
 * To mock dependencies in ESM, you can create fixtures that export mock
 * functions and objects. For example, the core module is mocked in this test,
 * so that the actual '@actions/core' module is not imported.
 */
import { jest } from '@jest/globals'
import * as core from '../__fixtures__/core.js'
import { wait } from '../__fixtures__/wait.js'
import * as gcf from '../__fixtures__/functions.js'
import * as cp from '../__fixtures__/child_process'
import { Cache } from '../src/cache.js'
import { TaskStatusResponse } from '../src/interfaces.js'

// Mocks should be declared before the module being tested is imported.
jest.unstable_mockModule('@actions/core', () => core)
jest.unstable_mockModule('../src/wait.js', () => ({ wait }))
jest.unstable_mockModule('../src/functions.js', () => gcf)
jest.unstable_mockModule('child_process', () => cp)

// The module being tested should be imported dynamically. This ensures that the
// mocks are used in place of any actual dependencies.
const { run } = await import('../src/main.js')

const cache = new Cache('./tmp')

let output = {
  task_id: 't1',
  status: 'success',
  result: '{"stdout": "res", "stderr": "stderr" }',
  completion_t: '1',
  exception: 'ex',
  details: {}
}
describe('main.ts', () => {
  beforeAll(() => {
    process.env['GITHUB_REPOSITORY'] = 'owner/repo'
  })
  beforeEach(() => {
    // Set the action's inputs as return values from core.getInput().
    core.getInput.mockImplementation(function (name: string): string {
      if (name === 'client_id') {
        return '1'
      } else if (name === 'client_secret') {
        return '2'
      } else if (name === 'endpoint_uuid') {
        return '3'
      } else if (name === 'function_uuid') {
        return '4'
      } else if (name === 'args') {
        return '[]'
      } else if (name === 'kwargs' || name === 'user_endpoint_config') {
        return '{}'
      } else if (name === 'resource_specification') {
        return '{ "num_nodes": 1 }'
      } else {
        return ''
      }
    })

    gcf.getToken.mockImplementation(() =>
      Promise.resolve({
        access_token: '1',
        scope: 'scope',
        resource_server: 'rs',
        expires_in: 1,
        other_tokens: []
      })
    )

    gcf.submit_tasks.mockImplementation(() =>
      Promise.resolve({
        request_id: 'ri1',
        task_group_id: 'tg1',
        endpoint_id: 'euuid',
        tasks: { t: ['task1'] }
      })
    )

    gcf.check_status.mockImplementation(() => Promise.resolve(output))

    gcf.register_function.mockImplementation(() =>
      Promise.resolve({ function_uuid: '1234' })
    )

    // Mock the wait function so that it does not actually wait.
    wait.mockImplementation(() => Promise.resolve('done!'))

    cp.execSync.mockReturnValueOnce('res')
  })

  afterEach(() => {
    jest.resetAllMocks()
  })

  it('Check that main function runs with inputs', async () => {
    cp.execSync.mockReset()
    cp.execSync.mockReturnValue(output.result)
    await run()
    expect(core.setOutput).toHaveBeenCalledWith('response', output)
    expect(core.setOutput).toHaveBeenCalledWith('result', output.result)
  })

  it('Test missing client_id and client_secret', async () => {
    core.getInput.mockReset()
    core.getInput.mockImplementation(function (name: string): string {
      if (name === 'client_secret') {
        return '2'
      } else if (name === 'endpoint_uuid') {
        return '3'
      } else if (name === 'function_uuid') {
        return '4'
      } else if (name === 'args') {
        return '[]'
      } else if (name === 'kwargs' || name === 'user_endpoint_config') {
        return '{}'
      } else if (name === 'resource_specification') {
        return '{ "num_nodes": 1 }'
      } else {
        return ''
      }
    })

    await run()
    expect(core.setFailed).toHaveBeenCalledWith(
      Error('CLIENT_ID or CLIENT_SECRET has not been specified')
    )
  })

  it('Validate that main function fails when adequate inputs are not provided', async () => {
    core.getInput.mockReset()
    core.getInput.mockImplementation(function (name: string): string {
      if (name === 'client_id') {
        return '1'
      } else if (name === 'client_secret') {
        return '2'
      } else if (name === 'endpoint-uuid') {
        return '3'
      } else if (name === 'args') {
        return '[]'
      } else if (name == 'kwargs' || name == 'user_endpoint_config') {
        return '{}'
      } else if (name === 'resource_specification') {
        return '{ "num_nodes": 1 }'
      } else {
        return ''
      }
    })
    await run()
    expect(core.setFailed).toHaveBeenCalledWith(
      Error('Either shell_cmd or function_uuid must be specified')
    )
  })

  it('Test registration of shell cmd', async () => {
    const reg_result: TaskStatusResponse = {
      task_id: 't1',
      status: 'success',
      result: JSON.stringify({
        python_version: '1.2.3',
        sdk_version: '123',
        serde_identifier: '10'
      }),
      completion_t: '1',
      exception: 'ex',
      details: {}
    }
    //cp.execSync.mockReturnValueOnce(reg_result)
    //cp.execSync.mockReturnValueOnce(reg_result) // execSync called twice
    gcf.check_status.mockReset()
    gcf.check_status.mockImplementation(() => Promise.resolve(reg_result))
    core.getInput.mockReset()
    core.getInput.mockImplementation(function (name: string): string {
      if (name === 'client_id') {
        return '1'
      } else if (name === 'client_secret') {
        return '2'
      } else if (name === 'endpoint_uuid') {
        return '3'
      } else if (name === 'shell_cmd') {
        return 'pip freeze'
      } else if (name === 'args') {
        return '[]'
      } else if (name === 'kwargs' || name == 'user_endpoint_config') {
        return '{}'
      } else if (name === 'resource_specification') {
        return '{ "num_nodes": 1 }'
      } else {
        return ''
      }
    })
    await run()
    expect(core.setOutput).toHaveBeenCalledWith('response', reg_result)
    expect(core.setOutput).toHaveBeenCalledWith('result', reg_result.result)
  })

  it('Add coverage for JSON output types', async () => {
    let result = JSON.stringify({ stdout: 'stdout', stderr: 'stderr' })
    output.result = result

    gcf.check_status.mockReset()
    gcf.check_status.mockImplementation(() => Promise.resolve(output))
    cp.execSync.mockReset()
    cp.execSync.mockReturnValue(result)
    await run()
    expect(core.setOutput).toHaveBeenCalledWith('response', output)
    expect(core.setOutput).toHaveBeenCalledWith('result', result)

    result = JSON.stringify({ stdout: 'stdout', stderr: 'stderr' })
    cp.execSync.mockReset()
    cp.execSync.mockReturnValue(result)
    await run()
    expect(core.setOutput).toHaveBeenCalledWith('response', output)
    expect(core.setOutput).toHaveBeenCalledWith('result', result)
  })

  it('Check that error is raised in case of nonzero returncode', async () => {
    const result_dict = {
      stdout: 'stdout',
      stderr: 'stderr',
      returncode: 2
    }
    const result = JSON.stringify(result_dict)
    output.result = result

    gcf.check_status.mockReset()
    gcf.check_status.mockImplementation(() => Promise.resolve(output))

    cp.execSync.mockReset()
    cp.execSync.mockReturnValue(result)
    await run()

    expect(core.setFailed).toHaveBeenCalledWith(
      Error(result_dict['stdout'] + '\n' + result_dict['stderr'])
    )
  })

  it("Check that output result isn't deserialized when task fails", async () => {
    output = {
      task_id: 't1',
      status: 'failed',
      result: 'res',
      completion_t: '1',
      exception: 'ex',
      details: {}
    }

    const tsr: TaskStatusResponse = {
      task_id: 'tid111',
      status: 'success',
      result: 'Operation successful',
      completion_t: '100',
      exception: 'Failed task',
      details: {}
    }

    gcf.check_status.mockImplementationOnce(() => Promise.resolve(tsr))
    await run()
    expect(core.setOutput).toHaveBeenCalledWith('response', output)
    expect(core.setOutput).toHaveBeenCalledWith('result', '')
  })

  it('Ensure check_status for git clone fails', async () => {
    const tsr: TaskStatusResponse = {
      task_id: 'tid111',
      status: 'failed',
      result: 'Operation successful',
      completion_t: '100',
      exception: 'Failed task',
      details: {}
    }

    gcf.check_status.mockImplementationOnce(() => Promise.resolve(tsr))
    await run()
    expect(core.setFailed).toHaveBeenCalledWith(Error('Failed task'))
  })

  it('Remove cache and unset getToken mock', async () => {
    await cache.remove('access-token')
    gcf.getToken
      .mockClear()
      .mockRejectedValueOnce(new Error('function execution did not pass'))
    await run()
    expect(core.setFailed).toHaveBeenCalledWith(
      Error('function execution did not pass')
    )
  })
})
