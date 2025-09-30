export interface Token {
    access_token: string;
    scope: string;
    resource_server: string;
    expires_in: number;
    other_tokens: Array<string>;
}
export interface TaskStatusResponse {
    task_id: string;
    status: string;
    result: string;
    completion_t: string;
    exception: string;
    details: object;
}
export interface TaskSubmission {
    request_id: string;
    task_group_id: string;
    endpoint_id: string;
    tasks: object;
}
export interface RegisterResponse {
    function_uuid: string;
}
