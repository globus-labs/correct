import * as core from '@actions/core'
import * as github from '@actions/github'
import { Token } from './interfaces.js'
import {
  getToken,
  submit_tasks,
  check_status,
  register_function
} from './functions.js'
import { execSync } from 'child_process'
import { Cache } from './cache.js'
import * as path from 'path'
import fs from 'fs'

/**
 * The main function for the action.
 *
 * @returns Resolves when the action is complete.
 */
export async function run(): Promise<void> {
  try {
    const CLIENT_ID: string = core.getInput('client_id')
    const CLIENT_SECRET: string = core.getInput('client_secret')
    const endpoint_uuid: string = core.getInput('endpoint_uuid')
    let function_uuid: string = core.getInput('function_uuid')
    const shell_cmd: string = core.getInput('shell_cmd')
    let clone_endpoint_config: string = core.getInput('clone_endpoint_config')
    const user_endpoint_config: string = core.getInput('user_endpoint_config')
    const resource_specification: string = core.getInput(
      'resource_specification'
    )

    if (CLIENT_ID === '' || CLIENT_SECRET === '') {
      throw Error('CLIENT_ID or CLIENT_SECRET has not been specified')
    }

    if (clone_endpoint_config === '') {
      clone_endpoint_config = user_endpoint_config
    }
    const clone_conf = JSON.parse(clone_endpoint_config)
    const endpoint_config = JSON.parse(user_endpoint_config)
    const resource_spec = JSON.parse(resource_specification)

    if (function_uuid === '' && shell_cmd === '') {
      throw Error('Either shell_cmd or function_uuid must be specified')
    }

    const args: string = core.getInput('args')
    const kwargs: string = core.getInput('kwargs')

    // install globus-compute-sdk if not already installed
    execSync(
      'gc_installed=$(pip freeze | grep globus-compute-sdk | wc -l) &&' +
        ' if [ ${gc_installed} -lt 1 ]; then pip install globus-compute-sdk; fi;'
    )

    const cache = new Cache(path.resolve('./tmp'))

    let access_token = await cache.get('access-token')

    if ((await cache.get('access-token')) == null) {
      console.log('Token not cached. Requesting new token')
      const token: Token = await getToken(CLIENT_ID, CLIENT_SECRET)
      await cache.set('access-token', token.access_token)
      access_token = token.access_token
    } else {
      console.log('Reusing existing token')
      access_token = await cache.get('access-token')
    }

    // Clone git repo with GC function
    const branch = github.context.ref
    const repo = github.context.repo
    const tmp_workdir = 'gc-action-temp'
    // const tmp_repodir = `${tmp_workdir}/${repo.repo}`

    const url = `${github.context.serverUrl}/${repo.owner}/${repo.repo}`
    console.log(`Cloning repo ${url}`)
    const cmd = `if [ -d ${tmp_workdir} ]; then rm -r ${tmp_workdir}; fi && mkdir ${tmp_workdir} && cd ${tmp_workdir} && git clone ${url} && cd ${repo.repo} && git checkout ${branch}`
    console.log('Registering function')
    const clone_reg = await register_function(access_token, cmd)
    const clone_uuid = clone_reg.function_uuid

    console.log(`Submitting function ${clone_uuid} to clone repo`)
    const sub_res = await submit_tasks(
      access_token,
      endpoint_uuid,
      clone_conf,
      resource_spec,
      clone_uuid,
      '[]',
      '{}'
    )

    const clone_key: string = Object.keys(sub_res.tasks)[0]
    const clone_task: string = sub_res.tasks[clone_key as keyof object][0]
    console.log('Checking for results')
    const clone_res = await check_status(access_token, clone_task)
    if (clone_res.status !== 'success') {
      console.log(clone_res)
      throw Error(clone_res.exception)
    } else {
      // write output to file and deserialize
      const serialized_out = 'serialized_clone.out'
      fs.writeFileSync(serialized_out, clone_res.result)

      const clone_output = execSync(
        `python -c 'import globus_compute_sdk; import json;` +
          ` f = open("${serialized_out}", "r");` +
          ` serialized_data = f.read();` +
          ` f.close();` +
          ` data = globus_compute_sdk.serialize.concretes.DillDataBase64().deserialize(f"{serialized_data}");` +
          ` print(json.dumps({"stdout": data.stdout, "stderr": data.stderr, "cmd": data.cmd, "returncode": data.returncode})` +
          ` if hasattr(data, "stdout") else json.dumps(data).replace("\\n", ""), end="")'`,
        { encoding: 'utf-8' }
      )

      try {
        const clone_json = JSON.parse(clone_output)
        console.log(clone_json.stdout)
        console.error(clone_json.stderr)
      } catch (e) {
        console.error(e)
        console.log(clone_output)
      }
    }

    //const cmd = `mkdir gc-action-temp; cd gc-action-temp; git clone ${}`

    if (shell_cmd.length !== 0) {
      const reg_response = await register_function(access_token, shell_cmd)
      function_uuid = reg_response.function_uuid
    }

    const output_stdout: string = `${function_uuid}-action_output.stdout`
    const output_stderr: string = `${function_uuid}-action_output.stderr`

    const batch_res = await submit_tasks(
      access_token,
      endpoint_uuid,
      endpoint_config,
      resource_spec,
      function_uuid,
      args,
      kwargs
    )

    const keys: string = Object.keys(batch_res.tasks)[0]
    const task_uuid: string = batch_res.tasks[keys as keyof object][0]
    const response = await check_status(access_token, task_uuid)

    core.setOutput('stdout', output_stdout)
    core.setOutput('stderr', output_stderr)
    core.setOutput('response', response)

    if (response.status === 'success') {
      const data = response.result

      // write script to file
      const serialized_file = 'serialized_data.out'
      fs.writeFileSync(serialized_file, data)

      const output = execSync(
        `python -c 'import globus_compute_sdk; import json;` +
          ` f = open("${serialized_file}", "r");` +
          ` serialized_data = f.read();` +
          ` f.close();` +
          ` data = globus_compute_sdk.serialize.concretes.DillDataBase64().deserialize(f"{serialized_data}");` +
          ` print(json.dumps({"stdout": data.stdout, "stderr": data.stderr, "cmd": data.cmd, "returncode": data.returncode})` +
          ` if hasattr(data, "stdout") else json.dumps(data).replace("\\n", ""), end="")'`,
        { encoding: 'utf-8' }
      )

      core.setOutput('result', output)

      const output_json = JSON.parse(output)

      if ('stdout' in output_json) {
        if ('returncode' in output_json && output_json['returncode'] != 0) {
          fs.writeFileSync(output_stdout, output_json['stdout'])
          fs.writeFileSync(output_stderr, output_json['stderr'])
          throw Error(output_json['stdout'] + '\n' + output_json['stderr'])
        }
        console.log(output_json['stdout'])
        fs.writeFileSync(output_stdout, output_json['stdout'])
        fs.writeFileSync(output_stderr, output_json['stderr'])
      } else {
        console.error(output_json)
        fs.writeFileSync(output_stdout, '\n')
        fs.writeFileSync(output_stderr, output)
      }
    } else {
      core.setOutput('result', '')
    }
  } catch (error) {
    core.setFailed(error as Error)
  }
}
