export declare class Cache {
    dir: string;
    constructor(dir: string);
    set(key: string, value: string): Promise<string>;
    get(key: string): Promise<string | null>;
    remove(key: string): Promise<void>;
}
