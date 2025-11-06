import json
import os
import sys
import typing as t

from time import sleep
from time import time
from uuid import uuid4
from diaspora_event_sdk import block_until_ready
from diaspora_event_sdk import Client as GlobusClient
from diaspora_event_sdk import KafkaConsumer
from diaspora_event_sdk import KafkaProducer

from globus_compute_sdk import Executor
from globus_compute_sdk.sdk.shell_function import ShellFunction


def octopus_setup(client_id: str, client_secret: str) -> t.Tuple[str, str, str]:

    os.environ["DIASPORA_SDK_CLIENT_ID"] = client_id
    os.environ["DIASPORA_SDK_CLIENT_SECRET"] = client_secret
    c = GlobusClient()
    octo_dict = c.retrieve_key()
    assert block_until_ready()
    topic = "topic" + c.subject_openid[-12:] + "1"
    c.register_topic(topic=topic)
    sleep(5)
    # c.register_topic(topic=topic)
    return topic, str(uuid4()), octo_dict


class OctopusShellFunction(ShellFunction):

    from globus_compute_sdk.sdk.shell_function import ShellResult

    def __init__(
        self,
        cmd,
        octo_dict,
        topic,
        run_uuid,
        stdout=None,
        stderr=None,
        walltime=None,
        snippet_lines=1000,
        name=None,
        return_dict=False,
    ):
        super().__init__(
            cmd, stdout, stderr, walltime, snippet_lines, name, return_dict
        )

        # TODO: remove eventually or fix
        self.access_key = octo_dict["access_key"]
        self.secret_key = octo_dict["secret_key"]
        self.endpoint = octo_dict["endpoint"]

        self.client_id = os.getenv("GLOBUS_COMPUTE_CLIENT_ID")
        self.client_secret = os.getenv("GLOBUS_COMPUTE_CLIENT_SECRET")
        self.topic = topic
        self.uuid = run_uuid

    def execute_cmd_line(self, cmd: str) -> t.Union[ShellResult, dict]:
        import os
        import subprocess
        import tempfile
        import uuid
        from time import time
        from diaspora_event_sdk import block_until_ready
        from diaspora_event_sdk import Client as GlobusClient
        from diaspora_event_sdk import KafkaProducer
        from globus_compute_sdk.sdk.shell_function import ShellResult

        sandbox_error_message = None

        # TODO: Fix this
        os.environ["DIASPORA_SDK_CLIENT_ID"] = self.client_id
        os.environ["DIASPORA_SDK_CLIENT_SECRET"] = self.client_secret
        os.environ["OCTOPUS_AWS_ACCESS_KEY_ID"] = self.access_key
        os.environ["OCTOPUS_AWS_SECRET_ACCESS_KEY"] = self.secret_key
        os.environ["OCTOPUS_BOOTSTRAP_SERVERS"] = self.endpoint

        # c = GlobusClient()
        # c.retrieve_key()
        # assert block_until_ready()
        producer = KafkaProducer()

        run_dir = None
        # run_dir takes priority over sandboxing
        if os.environ.get("GC_TASK_SANDBOX_DIR"):
            run_dir = os.environ["GC_TASK_SANDBOX_DIR"]
        else:
            sandbox_error_message = (
                "WARNING: Task sandboxing will not work due to "
                "endpoint misconfiguration. Please enable sandboxing "
                "on the remote endpoint. "
            )

        if run_dir:
            os.makedirs(run_dir, exist_ok=True)
            os.chdir(run_dir)

        # For backward compatibility with older endpoints which don't export
        # GC_TASK_UUID, a truncated uuid string will be used as prefix
        prefix = os.environ.get("GC_TASK_UUID", str(uuid.uuid4())[-10:]) + "."

        stdout = (
            self.stdout
            or tempfile.NamedTemporaryFile(
                dir=os.getcwd(), prefix=prefix, suffix=".stdout"
            ).name
        )
        stderr = (
            self.stderr
            or tempfile.NamedTemporaryFile(
                dir=os.getcwd(), prefix=prefix, suffix=".stderr"
            ).name
        )
        std_out = self.open_std_fd(stdout)
        std_err = self.open_std_fd(stderr)
        exception_name = None

        if sandbox_error_message:
            print(sandbox_error_message, file=std_err)

        try:
            start = time()
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                shell=True,
                executable="/bin/bash",
                close_fds=False,
            )

            for c in iter(lambda: proc.stdout.readline(), b""):
                std_out.buffer.write(c)
                producer.send(
                    self.topic, {"uuid": self.uuid, "message": c.decode("utf-8")}
                )
                if self.walltime is not None and time() - start >= self.walltime:
                    raise subprocess.TimeoutExpired()

            proc.poll()
            returncode = proc.returncode
            if returncode != 0:
                exception_name = "subprocess.CalledProcessError"

        except subprocess.TimeoutExpired:
            # Returncode to match behavior of timeout bash command
            # https://man7.org/linux/man-pages/man1/timeout.1.html
            returncode = 124
            exception_name = "subprocess.TimeoutExpired"

        finally:
            producer.send(
                self.topic, {"uuid": self.uuid, "message": "KafkaProducerEnd"}
            )
            producer.close()
            stdout_snippet, stderr_snippet = self.get_and_close_streams(
                std_out, std_err
            )

        result = {
            "cmd": cmd,
            "stdout": stdout_snippet,
            "stderr": stderr_snippet,
            "returncode": returncode,
            "exception_name": exception_name,
        }
        if self.return_dict:
            return result
        return ShellResult(**result)  # type: ignore[arg-type]


def main():
    endpoint_id = sys.argv[1]
    cmd = sys.argv[2]
    endpoint_config = json.loads(sys.argv[3])

    client_id = os.getenv("client_id")
    client_secret = os.getenv("client_secret")

    print(f"Running command: {cmd}", file=sys.stderr)

    topic, run_uuid, octo_dict = octopus_setup(
        client_id=client_id, client_secret=client_secret
    )
    bf = OctopusShellFunction(
        cmd,
        octo_dict,
        topic,
        run_uuid,
        snippet_lines=100,
    )

    with Executor(endpoint_id=endpoint_id, user_endpoint_config=endpoint_config) as gce:

        consumer = KafkaConsumer(
            topic,
            auto_offset_reset="latest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        future = gce.submit(bf)

        try:
            while not future.done():
                messages = consumer.poll(timeout_ms=100)
                for tp, records in messages.items():
                    for rcrd in records:
                        msg = rcrd.value
                        if "uuid" in msg and msg["uuid"] == run_uuid:
                            if msg["message"] == "KafkaProducerEnd":
                                break
                            print(msg["message"], end="", file=sys.stderr)
        except Exception as e:
            print("failes")
        finally:
            consumer.close()

        result = future.result()
        print(
            json.dumps({"returncode": result.returncode, "stdout": result.stdout}),
            flush=True,
        )


if __name__ == "__main__":
    main()
