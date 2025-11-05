import {
  Token,
  TaskStatusResponse,
  TaskSubmission,
  RegisterResponse
} from './interfaces.js'
import { execSync } from 'child_process'
import { exponential_decay } from './decay.js'
import { Buffer } from 'buffer'
import { validate as isValidUUID } from 'uuid'

/**
 * Retrieve bearer tokens from Globus Auth
 *
 * @param CLIENT_ID:  The Globus Client ID to use
 * @param CLIENT_SECRET: The secret associated with the Globus Client ID
 * @returns TokenResponse information returned by Globus Auth
 *
 */
export function getToken(
  CLIENT_ID: string,
  CLIENT_SECRET: string
): Promise<Token> {
  const token_fmt: string = `${CLIENT_ID}:${CLIENT_SECRET}`
  const basic_token: string = Buffer.from(token_fmt, 'utf-8').toString('base64')

  const gcscope: string =
    'https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all'
  const gcgrant_type: string = 'client_credentials'

  const headers: Headers = new Headers()
  headers.set('Content-Type', 'application/x-www-form-urlencoded')
  headers.set('Authorization', `Basic ${basic_token}`)

  const url: URL = new URL('/v2/oauth2/token', 'https://auth.globus.org')
  url.search = new URLSearchParams({
    scope: gcscope,
    grant_type: gcgrant_type
  }).toString()

  console.log('Issuing request: ' + url)
  const request: Request = new Request(url, {
    method: 'POST',
    headers: headers
  })

  return fetch(request)
    .then((res) => {
      if (res.ok) {
        return res.json()
      }
      return res.text().then((text) => {
        throw new Error(text)
      })
    })
    .then((res) => {
      return res as Token
    })
}

/**
 *
 * @param shell_cmd command to register with Globus Compute
 * @returns Registered function UUID
 */
export function register_function(
  access_token: string | null,
  shell_cmd: string
): Promise<RegisterResponse> {
  const serialized_body = execSync(
    `python -c 'import json; import sys; import globus_compute_sdk;` +
      ` data = globus_compute_sdk.serialize.concretes.CombinedCode().serialize(globus_compute_sdk.sdk.shell_function.ShellFunction("${shell_cmd}", snippet_lines=50000, return_dict=True));` +
      ` print(json.dumps({"function_name": "ci_shell_cmd", "function_code": f"{len(data)}\\n{data}", "meta":` +
      ` { "python_version":  ".".join(str(v) for v in sys.version_info[0:3]),` +
      ` "sdk_version": globus_compute_sdk.__version__, "serde_identifier": "10"}}))'`,
    { encoding: 'utf-8' }
  )

  const headers: Headers = new Headers()
  headers.set('Content-Type', 'application/json')
  headers.set('Authorization', `Bearer ${access_token}`)

  const url: URL = new URL(`/v3/functions`, 'https://compute.api.globus.org')

  const request: Request = new Request(url, {
    method: 'POST',
    headers: headers,
    body: serialized_body
  })

  return fetch(request)
    .then((res) => {
      if (res.ok) {
        return res.json()
      }
      return res.text().then((text) => {
        throw new Error(text)
      })
    })
    .then((res) => {
      return res as RegisterResponse
    })
}

/**
 * Submit functions with given arguments to the specified Globus Compute Endpoint
 *
 * @param access_token : Globus Auth bearer token
 * @param endpoint_uuid : Globus Compute Endpoint to send tasks to
 * @param function_uuid  : Globus Compute registered function UUID
 * @param args : Arguments to provide to tasks
 * @param kwargs : Keyword arguments to provide to task
 * @returns Task submission response returned by Globus Compute
 *
 */
export function submit_tasks(
  access_token: string | null,
  endpoint_uuid: string,
  user_endpoint_config: { [key: string]: string | number | object },
  resource_specification: { [key: string]: string | number | object },
  function_uuid: string,
  args: string,
  kwargs: string
): Promise<TaskSubmission> {
  // checking if valid type
  if (!isValidUUID(endpoint_uuid)) {
    throw new Error(`Endpoint UUID ${endpoint_uuid} is not a valid UUID`)
  } else if (!isValidUUID(function_uuid)) {
    throw new Error(`Function UUID ${function_uuid} is not a valid UUID`)
  }

  // configure inputs to be Globus Compute JSONData or accept serialized kwargs
  const JSONDataSerdeID = '11'
  const a = `${JSONDataSerdeID}\n${args}`

  // check if args and kwargs are valid JSON
  JSON.parse(args)
  JSON.parse(kwargs)

  const k = `${JSONDataSerdeID}\n${kwargs}`
  const serde_args = `${a.length}\n${a}${k.length}\n${k}`

  const headers: Headers = new Headers()
  headers.set('Content-Type', 'application/json')
  headers.set('Authorization', `Bearer ${access_token}`)

  const body: { [key: string]: string | number | object } = {
    tasks: {
      [function_uuid]: [serde_args]
    }
  }

  body['user_endpoint_config'] = user_endpoint_config
  body['result_serializers'] = ['globus_compute_sdk.serialize.JSONData']

  if (Object.keys(resource_specification).length !== 0) {
    body['resource_specification'] = resource_specification
  }

  const content_len = JSON.stringify(body).length
  headers.set('content-length', content_len.toString())

  const url: URL = new URL(
    `/v3/endpoints/${endpoint_uuid}/submit`,
    'https://compute.api.globus.org'
  )

  console.log('Issuing request: ' + url)
  const request: Request = new Request(url, {
    method: 'POST',
    headers: headers,
    body: JSON.stringify(body)
  })

  return fetch(request)
    .then((res) => {
      if (res.ok) {
        return res.json()
      }
      return res.text().then((text) => {
        throw new Error(text)
      })
    })
    .then((res) => {
      return res as TaskSubmission
    })
}

/**
 * Check the status of a running task
 *
 * @param access_token : Globus Auth bearer token
 * @param task_uuid : Task UUID of running Globus Compute task
 * @returns TaskStatusResponse returned by Globus Compute
 */
export function check_status(
  access_token: string | null,
  task_uuid: string
): Promise<TaskStatusResponse> {
  const headers: Headers = new Headers()
  headers.set('Content-Type', 'application/json')
  headers.set('Authorization', `Bearer ${access_token}`)

  const url: URL = new URL(
    `/v2/tasks/${task_uuid}`,
    'https://compute.api.globus.org'
  )

  // console.log('Issuing request' + url)
  const request: Request = new Request(url, {
    method: 'GET',
    headers: headers
  })

  const wait_for_ep = async function (): Promise<TaskStatusResponse> {
    while (true) {
      await exponential_decay()
      console.log('Issuing request : ' + url)
      const response: Response = await fetch(request)
      if (!response.ok) {
        throw new Error(await response.text())
      }

      const results: TaskStatusResponse =
        (await response.json()) as TaskStatusResponse

      console.log('Result status:' + results.status)
      // just for testing
      if (
        ['success', 'failed'].indexOf(results.status.toLowerCase()) == -1 &&
        results.task_id === 'testing'
      ) {
        return results
        //  }
      } else if (results.status === 'failed') {
        throw new Error(results.exception)
      } else if (results.status === 'success') {
        return results
      }
    }
  }
  return wait_for_ep()
}
