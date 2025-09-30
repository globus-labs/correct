import { Token, TaskStatusResponse, TaskSubmission, RegisterResponse } from './interfaces.js';
/**
 * Retrieve bearer tokens from Globus Auth
 *
 * @param CLIENT_ID:  The Globus Client ID to use
 * @param CLIENT_SECRET: The secret associated with the Globus Client ID
 * @returns TokenResponse information returned by Globus Auth
 *
 */
export declare function getToken(CLIENT_ID: string, CLIENT_SECRET: string): Promise<Token>;
/**
 *
 * @param shell_cmd command to register with Globus Compute
 * @returns Registered function UUID
 */
export declare function register_function(access_token: string | null, shell_cmd: string): Promise<RegisterResponse>;
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
export declare function submit_tasks(access_token: string | null, endpoint_uuid: string, user_endpoint_config: {
    [key: string]: string | number | object;
}, resource_specification: {
    [key: string]: string | number | object;
}, function_uuid: string, args: string, kwargs: string): Promise<TaskSubmission>;
/**
 * Check the status of a running task
 *
 * @param access_token : Globus Auth bearer token
 * @param task_uuid : Task UUID of running Globus Compute task
 * @returns TaskStatusResponse returned by Globus Compute
 */
export declare function check_status(access_token: string | null, task_uuid: string): Promise<TaskStatusResponse>;
