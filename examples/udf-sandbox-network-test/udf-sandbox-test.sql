CREATE OR REPLACE TEMPORARY FUNCTION sandboxTest(host string, port string)
RETURNS string
LANGUAGE PYTHON
AS $$
import subprocess

try:
    command = ['nc', '-zv', host, str(port)]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return str(result.returncode) + "|" + result.stdout.decode() + result.stderr.decode()
except Exception as e:
    return str(e)
$$;
