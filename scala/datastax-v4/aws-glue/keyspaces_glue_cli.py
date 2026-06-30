#!/usr/bin/env python3
"""CLI for managing Amazon Keyspaces AWS Glue jobs."""

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import boto3
import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(
    name="keyspaces-bulk-cli",
    help="CLI for Amazon Keyspaces AWS Glue jobs. Launch, monitor, and manage Glue job runs.",
    no_args_is_help=True,
)

console = Console()

CONFIG_FILE_NAME = ".keyspaces-bulk-cli.json"
DRIVER_CONF_DEFAULT = "keyspaces-application.conf"
FORMAT_DEFAULT = "parquet"
KEYSPACE_DEFAULT = "mykeyspace"
TABLE_DEFAULT = "mytable"

JOB_NAME_PREFIXES = [
    "AmazonKeyspacesExportToS3-",
    "AmazonKeyspacesImportFromS3-",
    "AmazonKeyspacesCount-",
    "AmazonKeyspacesBulkDelete-",
    "AmazonKeyspacesModifyTTL-",
    "AmazonKeyspacesIncrementalExportToS3-",
    "AmazonKeyspacesIncrementalImportToS3-",
    "AmazonKeyspacesTopPartitions-",
    "AmazonKeyspacesCompressPartition-",
    "AmazonKeyspacesGenerate-",
]

JOB_NAME_TEMPLATES = {
    "export": "AmazonKeyspacesExportToS3-{stack}",
    "import": "AmazonKeyspacesImportFromS3-{stack}",
    "count": "AmazonKeyspacesCount-{stack}",
    "bulk-delete": "AmazonKeyspacesBulkDelete-{stack}",
    "modify-ttl": "AmazonKeyspacesModifyTTL-{stack}",
    "incremental-export": "AmazonKeyspacesIncrementalExportToS3-{stack}",
    "incremental-import": "AmazonKeyspacesIncrementalImportToS3-{stack}",
    "top-partitions": "AmazonKeyspacesTopPartitions-{stack}",
    "compress-partition": "AmazonKeyspacesCompressPartition-{stack}",
    "generate": "AmazonKeyspacesGenerate-{stack}",
}

JOB_DEPLOY_CONFIG = {
    "export": {
        "cfn_stack_suffix": "export",
        "template": "export-to-s3/glue-job-export-to-s3.yaml",
        "script_src": "export-to-s3/export-sample.scala",
        "script_key": "{stack}-export.scala",
        "params": ["KeyspaceName", "TableName", "S3URI", "FORMAT"],
    },
    "import": {
        "cfn_stack_suffix": "import",
        "template": "import-from-s3/glue-job-import-from-s3.yaml",
        "script_src": "import-from-s3/import-sample.scala",
        "script_key": "{stack}-import.scala",
        "params": ["KeyspaceName", "TableName", "S3URI", "FORMAT"],
    },
    "count": {
        "cfn_stack_suffix": "count",
        "template": "count-table-rows/glue-job-count-rows.yaml",
        "script_src": "count-table-rows/count-example.scala",
        "script_key": "{stack}-count.scala",
        "params": ["KeyspaceName", "TableName"],
    },
    "bulk-delete": {
        "cfn_stack_suffix": "bulk-delete",
        "template": "bulk-delete/glue-job-bulk-delete.yaml",
        "script_src": "bulk-delete/bulk-delete-sample.scala",
        "script_key": "{stack}-bulk-delete.scala",
        "params": ["KeyspaceName", "TableName", "S3URI", "FORMAT"],
    },
    "modify-ttl": {
        "cfn_stack_suffix": "modify-ttl",
        "template": "modify-time-to-live/glue-job-modify-ttl.yaml",
        "script_src": "modify-time-to-live/modify-time-to-live.scala",
        "script_key": "{stack}-modify-ttl.scala",
        "params": ["KeyspaceName", "TableName"],
    },
    "incremental-export": {
        "cfn_stack_suffix": "inc-export",
        "template": "incremental-export-to-s3/glue-job-diff.yaml",
        "script_src": "incremental-export-to-s3/incremental-export-sample.scala",
        "script_key": "incremental-export-sample.scala",
        "params": ["KeyspaceName", "TableName", "S3URI", "FORMAT", "DISTINCTKEYS", "PASTURI", "CURRENTURI"],
    },
    "incremental-import": {
        "cfn_stack_suffix": "inc-import",
        "template": "incremental-export-to-s3/glue-job-incremental-import.yaml",
        "script_src": "incremental-export-to-s3/incremental-import-sample.scala",
        "script_key": "incremental-import-sample.scala",
        "params": ["KeyspaceName", "TableName", "S3URI", "FORMAT", "DISTINCTKEYS"],
    },
    "top-partitions": {
        "cfn_stack_suffix": "top-partitions",
        "template": "top-n-partitions/glue-job-top-partitions.yaml",
        "script_src": "top-n-partitions/top-partition-example.scala",
        "script_key": "{stack}-top-partitions.scala",
        "params": ["KeyspaceName", "TableName", "GroupBy", "MaxResults", "MinSize"],
    },
    "compress-partition": {
        "cfn_stack_suffix": "compress-partition",
        "template": "compress-partition/glue-job-compress-partition.yaml",
        "script_src": "compress-partition/compress-partition.scala",
        "script_key": "{stack}-compress-partition.scala",
        "params": ["KeyspaceName", "SourceTable", "TargetTable", "Compression"],
    },
}


def _find_config_file() -> Optional[Path]:
    """Search for config file in cwd, then home directory."""
    for directory in [Path.cwd(), Path.home()]:
        path = directory / CONFIG_FILE_NAME
        if path.exists():
            return path
    return None


def _read_config() -> dict:
    path = _find_config_file()
    if path:
        return json.loads(path.read_text())
    return {}


def _write_config(data: dict, path: Optional[Path] = None):
    if path is None:
        path = Path.cwd() / CONFIG_FILE_NAME
    path.write_text(json.dumps(data, indent=2) + "\n")


def _discover_stack_from_glue(region: Optional[str] = None, profile: Optional[str] = None) -> Optional[str]:
    """Query Glue for existing AmazonKeyspaces jobs and extract the stack name."""
    try:
        client = get_glue_client(region, profile)
        response = client.get_jobs()
        stacks = set()
        for job in response.get("Jobs", []):
            name = job["Name"]
            for prefix in JOB_NAME_PREFIXES:
                if name.startswith(prefix):
                    suffix = name[len(prefix):]
                    # Job name pattern: AmazonKeyspaces{Type}-{stack}
                    # The suffix after the prefix is the stack name
                    if suffix:
                        stacks.add(suffix)
                    break
        if len(stacks) == 1:
            return stacks.pop()
        if len(stacks) > 1:
            console.print(f"[yellow]Multiple stacks detected: {', '.join(sorted(stacks))}[/yellow]")
            console.print("[yellow]Use --stack to specify, or run 'keyspaces-bulk-cli config --stack <name>' to save.[/yellow]")
            return None
    except Exception:
        return None
    return None


def resolve_stack(stack_flag: Optional[str], region: Optional[str] = None, profile: Optional[str] = None) -> str:
    """Resolve stack name: --stack flag > env var > config file > auto-discover from Glue."""
    if stack_flag:
        return stack_flag

    env_stack = os.environ.get("KEYSPACES_GLUE_STACK")
    if env_stack:
        return env_stack

    cfg = _read_config()
    if cfg.get("stack"):
        return cfg["stack"]

    discovered = _discover_stack_from_glue(region, profile)
    if discovered:
        console.print(f"[dim]Auto-discovered stack: {discovered}[/dim]")
        return discovered

    console.print("[red]Could not determine stack name.[/red]")
    console.print("Set it with one of:")
    console.print("  --stack <name>")
    console.print("  export KEYSPACES_GLUE_STACK=<name>")
    console.print("  keyspaces-bulk-cli config --stack <name>")
    raise typer.Exit(1)


def resolve_job_name(command: str, job_name: Optional[str], stack: str) -> str:
    if job_name:
        return job_name
    template = JOB_NAME_TEMPLATES[command]
    return template.format(stack=stack)


def _job_exists(client, job_name: str) -> bool:
    try:
        client.get_job(JobName=job_name)
        return True
    except client.exceptions.EntityNotFoundException:
        return False


def _get_bucket_for_stack(session, stack: str) -> Optional[str]:
    cf = session.client("cloudformation")
    try:
        resp = cf.describe_stacks(StackName=stack)
        for output in resp["Stacks"][0].get("Outputs", []):
            if output.get("ExportName") == f"KeyspacesBucketNameExport-{stack}":
                return output["OutputValue"]
    except Exception:
        pass
    return None


def ensure_job_deployed(command: str, resolved_name: str, stack: str, region: Optional[str], profile: Optional[str]):
    """If the Glue job doesn't exist yet, deploy it via CloudFormation."""
    if command not in JOB_DEPLOY_CONFIG:
        return

    session = boto3.Session(region_name=region, profile_name=profile)
    glue = session.client("glue")

    if _job_exists(glue, resolved_name):
        return

    deploy_cfg = JOB_DEPLOY_CONFIG[command]
    console.print(f"[yellow]Job '{resolved_name}' not found. Deploying...[/yellow]")

    bucket = _get_bucket_for_stack(session, stack)
    if not bucket:
        console.print("[red]Cannot find S3 bucket from parent stack. Run 'bootstrap' first.[/red]")
        raise typer.Exit(1)

    script_dir = Path(__file__).resolve().parent
    script_src = script_dir / deploy_cfg["script_src"]
    script_key = "scripts/" + deploy_cfg["script_key"].format(stack=stack)

    s3 = session.client("s3")
    console.print(f"  Uploading script to s3://{bucket}/{script_key}...")
    s3.upload_file(str(script_src), bucket, script_key)

    cf = session.client("cloudformation")
    cfn_stack_name = f"{stack}-{deploy_cfg['cfn_stack_suffix']}"
    template_path = script_dir / deploy_cfg["template"]
    template_body = template_path.read_text()

    params = [{"ParameterKey": "ParentStack", "ParameterValue": stack}]
    default_param_values = {
        "KeyspaceName": KEYSPACE_DEFAULT,
        "TableName": TABLE_DEFAULT,
        "S3URI": f"s3://{bucket}/export",
        "FORMAT": FORMAT_DEFAULT,
        "DISTINCTKEYS": "key,value",
        "PASTURI": "s3://pasturi",
        "CURRENTURI": "s3://currenturi",
        "GroupBy": "pk",
        "MaxResults": "10",
        "MinSize": "1",
        "SourceTable": TABLE_DEFAULT,
        "TargetTable": f"{TABLE_DEFAULT}_compressed",
        "Compression": "ZSTD",
    }
    for p in deploy_cfg["params"]:
        params.append({"ParameterKey": p, "ParameterValue": default_param_values.get(p, "")})

    try:
        cf.create_stack(
            StackName=cfn_stack_name,
            TemplateBody=template_body,
            Parameters=params,
        )
        console.print(f"  Waiting for stack '{cfn_stack_name}'...")
        waiter = cf.get_waiter("stack_create_complete")
        waiter.wait(StackName=cfn_stack_name)
        console.print(f"  [green]Job deployed successfully.[/green]")
    except cf.exceptions.AlreadyExistsException:
        console.print(f"  [yellow]Stack '{cfn_stack_name}' already exists. Waiting for job to be available...[/yellow]")
        try:
            waiter = cf.get_waiter("stack_create_complete")
            waiter.wait(StackName=cfn_stack_name)
        except Exception:
            pass
    except Exception as e:
        console.print(f"  [red]Failed to deploy: {e}[/red]")
        raise typer.Exit(1)


def get_glue_client(region: Optional[str] = None, profile: Optional[str] = None):
    session = boto3.Session(region_name=region, profile_name=profile)
    return session.client("glue")


def start_job_run(
    client, job_name: str, arguments: dict, workers: Optional[int] = None, worker_type: Optional[str] = None
) -> str:
    params = {"JobName": job_name, "Arguments": arguments}
    if workers:
        params["NumberOfWorkers"] = workers
        params["WorkerType"] = worker_type or "G.2X"
    response = client.start_job_run(**params)
    return response["JobRunId"]


def format_arguments(args: dict) -> dict:
    return {f"--{k}": v for k, v in args.items() if v is not None and v != ""}


def _run_cmd(cmd: str, description: str, cwd: Optional[str] = None) -> bool:
    """Run a shell command, printing status. Returns True on success."""
    import subprocess
    console.print(f"  [dim]{description}...[/dim]")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=cwd)
    if result.returncode != 0:
        console.print(f"  [red]FAILED:[/red] {description}")
        if result.stderr:
            console.print(f"  [red]{result.stderr.strip()}[/red]")
        return False
    return True


def _check_command_exists(cmd: str) -> bool:
    import shutil
    return shutil.which(cmd) is not None


# --- Bootstrap Command ---


@app.command()
def bootstrap(
    stack: str = typer.Option("aksglue", help="CloudFormation stack name"),
    bucket: Optional[str] = typer.Option(None, help="S3 bucket name (default: amazon-keyspaces-bulk-cli-{stack}-{account_id})"),
    role_name: Optional[str] = typer.Option(None, help="IAM role name (default: amazon-keyspaces-bulk-cli-servcie-role-{stack})"),
    keyspace: str = typer.Option(KEYSPACE_DEFAULT, help="Default keyspace for jobs"),
    table: str = typer.Option(TABLE_DEFAULT, help="Default table for jobs"),
    s3_uri: Optional[str] = typer.Option(None, help="S3 URI for export/import (default: s3://{bucket}/export)"),
    format: str = typer.Option(FORMAT_DEFAULT, help="Default format: parquet, json, csv"),
    skip_jobs: bool = typer.Option(False, help="Only deploy infrastructure, skip Glue job creation"),
    region: Optional[str] = typer.Option(None, help="AWS region"),
    profile: Optional[str] = typer.Option(None, help="AWS profile"),
):
    """Bootstrap the full Keyspaces Glue infrastructure.

    Creates the CloudFormation stack (S3 bucket + IAM role), downloads and uploads
    connector JARs, builds the retry policy helper, uploads config files, and
    deploys the Export and Import Glue jobs.

    This is the Python equivalent of running setup-connector.sh.
    """
    import shutil
    import subprocess
    import tempfile

    # Validate prerequisites
    missing = []
    for cmd in ["aws", "git", "mvn"]:
        if not _check_command_exists(cmd):
            missing.append(cmd)
    if missing:
        console.print(f"[red]Missing required tools: {', '.join(missing)}[/red]")
        console.print("Install them before running bootstrap:")
        if "aws" in missing:
            console.print("  aws CLI: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html")
        if "git" in missing:
            console.print("  git: https://git-scm.com/book/en/v2/Getting-Started-Installing-Git")
        if "mvn" in missing:
            console.print("  maven: https://maven.apache.org/install.html")
        raise typer.Exit(1)

    # Get AWS account ID
    session = boto3.Session(region_name=region, profile_name=profile)
    sts = session.client("sts")
    try:
        account_id = sts.get_caller_identity()["Account"]
    except Exception as e:
        console.print(f"[red]Could not get AWS account ID: {e}[/red]")
        raise typer.Exit(1)

    # Resolve defaults
    bucket_name = bucket or f"amazon-keyspaces-bulk-cli-{stack}-{account_id}"
    iam_role_name = role_name or f"amazon-keyspaces-bulk-cli-servcie-role-{stack}"
    export_s3_uri = s3_uri or f"s3://{bucket_name}/export"

    console.print(f"\n[bold]Keyspaces Glue Bootstrap[/bold]")
    console.print(f"  Stack:    {stack}")
    console.print(f"  Bucket:   {bucket_name}")
    console.print(f"  Role:     {iam_role_name}")
    console.print(f"  Keyspace: {keyspace}")
    console.print(f"  Table:    {table}")
    console.print(f"  S3 URI:   {export_s3_uri}")
    console.print(f"  Format:   {format}")
    console.print()

    # Determine script directory (where the yaml/scala files live)
    script_dir = Path(__file__).resolve().parent

    # Step 1: Create base CloudFormation stack
    console.print("[bold]Step 1:[/bold] Creating base infrastructure (S3 bucket + IAM role)...")
    cf_client = session.client("cloudformation")
    template_path = script_dir / "glue-setup-template.yaml"
    template_body = template_path.read_text()

    try:
        cf_client.create_stack(
            StackName=stack,
            TemplateBody=template_body,
            Parameters=[
                {"ParameterKey": "KeyspacesBucketName", "ParameterValue": bucket_name},
                {"ParameterKey": "KeyspacesGlueServiceRoleName", "ParameterValue": iam_role_name},
            ],
            Capabilities=["CAPABILITY_NAMED_IAM"],
        )
        console.print("  Waiting for stack creation...")
        waiter = cf_client.get_waiter("stack_create_complete")
        waiter.wait(StackName=stack)
        console.print("  [green]Base stack created.[/green]")
    except cf_client.exceptions.AlreadyExistsException:
        console.print("  [yellow]Stack already exists, skipping creation.[/yellow]")
    except Exception as e:
        console.print(f"  [red]Stack creation failed: {e}[/red]")
        raise typer.Exit(1)

    # Step 2: Download and upload JARs
    console.print("\n[bold]Step 2:[/bold] Downloading and uploading connector JARs...")
    s3_client = session.client("s3")

    spark_connector_version = "3.1.0"
    spark_extensions_version = "2.8.0-3.4"
    sigv4_version = "4.0.9"
    scala_version = "2.12"

    jars = [
        {
            "name": f"spark-cassandra-connector-assembly_{scala_version}-{spark_connector_version}.jar",
            "url": f"https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_{scala_version}/{spark_connector_version}/spark-cassandra-connector-assembly_{scala_version}-{spark_connector_version}.jar",
        },
        {
            "name": f"spark-extension_{scala_version}-{spark_extensions_version}.jar",
            "url": f"https://repo1.maven.org/maven2/uk/co/gresearch/spark/spark-extension_{scala_version}/{spark_extensions_version}/spark-extension_{scala_version}-{spark_extensions_version}.jar",
        },
        {
            "name": f"aws-sigv4-auth-cassandra-java-driver-plugin-{sigv4_version}-shaded.jar",
            "url": f"https://repo1.maven.org/maven2/software/aws/mcs/aws-sigv4-auth-cassandra-java-driver-plugin/{sigv4_version}/aws-sigv4-auth-cassandra-java-driver-plugin-{sigv4_version}-shaded.jar",
        },
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
        for jar in jars:
            jar_path = Path(tmpdir) / jar["name"]
            console.print(f"  Downloading {jar['name']}...")
            result = subprocess.run(
                ["curl", "-L", "-f", "-o", str(jar_path), jar["url"]],
                capture_output=True, text=True,
            )
            if result.returncode != 0:
                console.print(f"  [red]Failed to download {jar['name']}[/red]")
                raise typer.Exit(1)

            console.print(f"  Uploading {jar['name']} to s3://{bucket_name}/jars/...")
            s3_client.upload_file(str(jar_path), bucket_name, f"jars/{jar['name']}")

        # Build and upload retry policy helper
        console.print("  Building amazon-keyspaces-helpers...")
        helpers_dir = Path(tmpdir) / "amazon-keyspaces-java-driver-helpers"
        result = subprocess.run(
            ["git", "clone", "https://github.com/aws-samples/amazon-keyspaces-java-driver-helpers", str(helpers_dir)],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            console.print(f"  [red]Failed to clone helpers repo[/red]")
            raise typer.Exit(1)

        result = subprocess.run(
            ["mvn", "clean", "package", "-Dmaven.test.skip=true", "-q"],
            capture_output=True, text=True, cwd=str(helpers_dir),
        )
        if result.returncode != 0:
            console.print(f"  [red]Failed to build helpers: {result.stderr[:200]}[/red]")
            raise typer.Exit(1)

        helpers_jar = helpers_dir / "target" / "amazon-keyspaces-helpers-1.0-SNAPSHOT.jar"
        console.print("  Uploading amazon-keyspaces-helpers-1.0-SNAPSHOT.jar...")
        s3_client.upload_file(str(helpers_jar), bucket_name, "jars/amazon-keyspaces-helpers-1.0-SNAPSHOT.jar")

    console.print("  [green]All JARs uploaded.[/green]")

    # Step 3: Upload config files
    console.print("\n[bold]Step 3:[/bold] Uploading configuration files...")
    for conf_file in ["cassandra-application.conf", "keyspaces-application.conf"]:
        conf_path = script_dir / conf_file
        if conf_path.exists():
            s3_client.upload_file(str(conf_path), bucket_name, f"conf/{conf_file}")
            console.print(f"  Uploaded {conf_file}")
        else:
            console.print(f"  [yellow]Skipped {conf_file} (not found)[/yellow]")

    if skip_jobs:
        console.print("\n[green]Infrastructure bootstrap complete (--skip-jobs specified).[/green]")
        _save_stack_config(stack)
        return

    # Step 4: Deploy Export Glue job
    console.print("\n[bold]Step 4:[/bold] Deploying Export to S3 Glue job...")
    export_stack_name = f"{stack}-export"
    export_script = script_dir / "export-to-s3" / "export-sample.scala"
    export_template = script_dir / "export-to-s3" / "glue-job-export-to-s3.yaml"

    s3_client.upload_file(
        str(export_script), bucket_name, f"scripts/{stack}-export.scala"
    )

    try:
        cf_client.create_stack(
            StackName=export_stack_name,
            TemplateBody=export_template.read_text(),
            Parameters=[
                {"ParameterKey": "ParentStack", "ParameterValue": stack},
                {"ParameterKey": "KeyspaceName", "ParameterValue": keyspace},
                {"ParameterKey": "TableName", "ParameterValue": table},
                {"ParameterKey": "S3URI", "ParameterValue": export_s3_uri},
                {"ParameterKey": "FORMAT", "ParameterValue": format},
            ],
        )
        console.print("  Waiting for export job stack...")
        waiter = cf_client.get_waiter("stack_create_complete")
        waiter.wait(StackName=export_stack_name)
        console.print("  [green]Export job deployed.[/green]")
    except cf_client.exceptions.AlreadyExistsException:
        console.print("  [yellow]Export stack already exists, skipping.[/yellow]")
    except Exception as e:
        console.print(f"  [red]Export stack failed: {e}[/red]")
        raise typer.Exit(1)

    # Step 5: Deploy Import Glue job
    console.print("\n[bold]Step 5:[/bold] Deploying Import from S3 Glue job...")
    import_stack_name = f"{stack}-import"
    import_script = script_dir / "import-from-s3" / "import-sample.scala"
    import_template = script_dir / "import-from-s3" / "glue-job-import-from-s3.yaml"

    s3_client.upload_file(
        str(import_script), bucket_name, f"scripts/{stack}-import.scala"
    )

    try:
        cf_client.create_stack(
            StackName=import_stack_name,
            TemplateBody=import_template.read_text(),
            Parameters=[
                {"ParameterKey": "ParentStack", "ParameterValue": stack},
                {"ParameterKey": "KeyspaceName", "ParameterValue": keyspace},
                {"ParameterKey": "TableName", "ParameterValue": table},
                {"ParameterKey": "S3URI", "ParameterValue": export_s3_uri},
                {"ParameterKey": "FORMAT", "ParameterValue": format},
            ],
        )
        console.print("  Waiting for import job stack...")
        waiter = cf_client.get_waiter("stack_create_complete")
        waiter.wait(StackName=import_stack_name)
        console.print("  [green]Import job deployed.[/green]")
    except cf_client.exceptions.AlreadyExistsException:
        console.print("  [yellow]Import stack already exists, skipping.[/yellow]")
    except Exception as e:
        console.print(f"  [red]Import stack failed: {e}[/red]")
        raise typer.Exit(1)

    # Step 6: Deploy Count Glue job
    console.print("\n[bold]Step 6:[/bold] Deploying Count Glue job...")
    _deploy_child_stack(cf_client, s3_client, script_dir, bucket_name, stack,
                        "count", "count-table-rows/glue-job-count-rows.yaml",
                        "count-table-rows/count-example.scala",
                        f"scripts/{stack}-count.scala",
                        [("KeyspaceName", keyspace), ("TableName", table)])

    _save_stack_config(stack)
    console.print(f"\n[bold green]Bootstrap complete![/bold green]")
    console.print(f"  Stack '{stack}' saved to config. You can now run jobs:")
    console.print(f"  ./keyspaces-bulk-cli export --keyspace {keyspace} --table {table} --s3-uri {export_s3_uri}")


def _deploy_child_stack(cf_client, s3_client, script_dir, bucket_name, parent_stack,
                        suffix, template_rel, script_rel, script_s3_key, extra_params):
    """Deploy a child CloudFormation stack for a Glue job."""
    cfn_stack_name = f"{parent_stack}-{suffix}"
    script_path = script_dir / script_rel
    template_path = script_dir / template_rel

    s3_client.upload_file(str(script_path), bucket_name, script_s3_key)

    params = [{"ParameterKey": "ParentStack", "ParameterValue": parent_stack}]
    for key, val in extra_params:
        params.append({"ParameterKey": key, "ParameterValue": val})

    try:
        cf_client.create_stack(
            StackName=cfn_stack_name,
            TemplateBody=template_path.read_text(),
            Parameters=params,
        )
        console.print(f"  Waiting for stack '{cfn_stack_name}'...")
        waiter = cf_client.get_waiter("stack_create_complete")
        waiter.wait(StackName=cfn_stack_name)
        console.print(f"  [green]{suffix} job deployed.[/green]")
    except cf_client.exceptions.AlreadyExistsException:
        console.print(f"  [yellow]{suffix} stack already exists, skipping.[/yellow]")
    except Exception as e:
        console.print(f"  [red]{suffix} stack failed: {e}[/red]")
        raise typer.Exit(1)


def _save_stack_config(stack: str):
    """Save the stack name to local config after bootstrap."""
    cfg = _read_config()
    cfg["stack"] = stack
    _write_config(cfg)
    console.print(f"  [dim]Saved stack '{stack}' to {Path.cwd() / CONFIG_FILE_NAME}[/dim]")


# --- Config Command ---


@app.command()
def config(
    stack: Optional[str] = typer.Option(None, help="Set the default stack name"),
    show: bool = typer.Option(False, help="Show current configuration"),
    discover: bool = typer.Option(False, help="Auto-discover stack from existing Glue jobs"),
    region: Optional[str] = typer.Option(None, help="AWS region (for discovery)"),
    profile: Optional[str] = typer.Option(None, help="AWS profile (for discovery)"),
):
    """Configure CLI defaults. Saves to .keyspaces-bulk-cli.json in the current directory."""
    if show:
        cfg = _read_config()
        env_stack = os.environ.get("KEYSPACES_GLUE_STACK")
        config_path = _find_config_file()

        table = Table(title="Keyspaces Glue CLI Configuration")
        table.add_column("Source", style="bold")
        table.add_column("Stack")
        table.add_column("Status")

        table.add_row("--stack flag", "", "[dim]not set[/dim]")
        table.add_row(
            "env KEYSPACES_GLUE_STACK",
            env_stack or "",
            "[green]active[/green]" if env_stack else "[dim]not set[/dim]",
        )
        table.add_row(
            f"config file ({config_path or 'not found'})",
            cfg.get("stack", ""),
            "[green]active[/green]" if cfg.get("stack") else "[dim]not set[/dim]",
        )
        console.print(table)
        console.print("\n[dim]Resolution order: --stack > env var > config file > auto-discover[/dim]")
        return

    if discover:
        discovered = _discover_stack_from_glue(region, profile)
        if discovered:
            cfg = _read_config()
            cfg["stack"] = discovered
            _write_config(cfg)
            console.print(f"[green]Discovered and saved stack: {discovered}[/green]")
        else:
            console.print("[red]No Keyspaces Glue jobs found to discover stack name.[/red]")
        return

    if stack:
        cfg = _read_config()
        cfg["stack"] = stack
        _write_config(cfg)
        console.print(f"[green]Saved stack: {stack}[/green]")
        console.print(f"  Config file: {Path.cwd() / CONFIG_FILE_NAME}")
        return

    console.print("Use --stack <name> to set, --show to view, or --discover to auto-detect.")


# --- Lifecycle Commands ---


def _resolve_job_and_run(module: str, job_name: Optional[str], run_id: Optional[str], region: Optional[str], profile: Optional[str]):
    """Resolve module/job name and optionally find the latest run."""
    if job_name:
        resolved_name = job_name
    else:
        stack = resolve_stack(None, region, profile)
        if module in JOB_NAME_TEMPLATES:
            resolved_name = JOB_NAME_TEMPLATES[module].format(stack=stack)
        else:
            resolved_name = module

    if not run_id:
        client = get_glue_client(region, profile)
        response = client.get_job_runs(JobName=resolved_name, MaxResults=1)
        runs_list = response.get("JobRuns", [])
        if not runs_list:
            console.print(f"[red]No runs found for job '{resolved_name}'[/red]")
            raise typer.Exit(1)
        run_id = runs_list[0]["Id"]

    return resolved_name, run_id


@app.command()
def status(
    module: str = typer.Argument(help="Module name (e.g. count, export, compress-partition)"),
    job_name: Optional[str] = typer.Option(None, help="Override: full Glue job name"),
    run_id: Optional[str] = typer.Option(None, help="Job run ID (defaults to latest run)"),
    region: Optional[str] = typer.Option(None, help="AWS region"),
    profile: Optional[str] = typer.Option(None, help="AWS profile"),
):
    """Get the status of a Glue job run."""
    resolved_name, resolved_run_id = _resolve_job_and_run(module, job_name, run_id, region, profile)
    client = get_glue_client(region, profile)
    response = client.get_job_run(JobName=resolved_name, RunId=resolved_run_id)
    run = response["JobRun"]

    table = Table(title=f"Job Run: {resolved_run_id}")
    table.add_column("Field", style="bold")
    table.add_column("Value")
    table.add_row("Job Name", run.get("JobName", ""))
    table.add_row("State", run.get("JobRunState", ""))
    table.add_row("Started", str(run.get("StartedOn", "")))
    table.add_row("Completed", str(run.get("CompletedOn", "")))
    table.add_row("Execution Time", f"{run.get('ExecutionTime', 0)}s")
    table.add_row("Workers", str(run.get("NumberOfWorkers", "")))
    table.add_row("Worker Type", run.get("WorkerType", ""))
    if run.get("ErrorMessage"):
        table.add_row("Error", run["ErrorMessage"])
    if run.get("Arguments"):
        table.add_row("Arguments", json.dumps(run["Arguments"], indent=2))

    console.print(table)


@app.command()
def logs(
    module: str = typer.Argument(help="Module name (e.g. count, export, compress-partition)"),
    job_name: Optional[str] = typer.Option(None, help="Override: full Glue job name"),
    run_id: Optional[str] = typer.Option(None, help="Job run ID (defaults to latest run)"),
    log_type: str = typer.Option("output", help="Log type: output or error"),
    limit: int = typer.Option(50, help="Number of log events to retrieve"),
    region: Optional[str] = typer.Option(None, help="AWS region"),
    profile: Optional[str] = typer.Option(None, help="AWS profile"),
):
    """Fetch CloudWatch logs for a Glue job run."""
    resolved_name, resolved_run_id = _resolve_job_and_run(module, job_name, run_id, region, profile)
    session = boto3.Session(region_name=region, profile_name=profile)
    logs_client = session.client("logs")

    if log_type == "error":
        log_group = "/aws-glue/jobs/error"
    else:
        log_group = "/aws-glue/jobs/output"

    log_stream = resolved_run_id

    try:
        response = logs_client.get_log_events(
            logGroupName=log_group,
            logStreamName=log_stream,
            limit=limit,
            startFromHead=False,
        )
        events = response.get("events", [])
        if not events:
            console.print(f"[yellow]No log events found for {resolved_run_id} in {log_group}[/yellow]")
            return

        for event in events:
            ts = datetime.fromtimestamp(event["timestamp"] / 1000, tz=timezone.utc)
            console.print(f"[dim]{ts.strftime('%H:%M:%S')}[/dim] {event['message'].rstrip()}")
    except logs_client.exceptions.ResourceNotFoundException:
        console.print(f"[red]Log stream {resolved_run_id} not found in {log_group}[/red]")


@app.command()
def cancel(
    module: str = typer.Argument(help="Module name (e.g. count, export, compress-partition)"),
    job_name: Optional[str] = typer.Option(None, help="Override: full Glue job name"),
    run_id: Optional[str] = typer.Option(None, help="Job run ID (defaults to latest run)"),
    region: Optional[str] = typer.Option(None, help="AWS region"),
    profile: Optional[str] = typer.Option(None, help="AWS profile"),
):
    """Cancel a running Glue job."""
    resolved_name, resolved_run_id = _resolve_job_and_run(module, job_name, run_id, region, profile)
    client = get_glue_client(region, profile)
    client.batch_stop_job_run(JobName=resolved_name, JobRunIds=[resolved_run_id])
    console.print(f"[green]Cancelled job run {resolved_run_id}[/green]")


@app.command()
def runs(
    module: str = typer.Argument(help="Module name (e.g. count, export, compress-partition)"),
    job_name: Optional[str] = typer.Option(None, help="Override: full Glue job name"),
    limit: int = typer.Option(10, help="Max runs to display"),
    output_json: bool = typer.Option(False, "--json", help="Output as JSON with job arguments (useful for triggers)"),
    region: Optional[str] = typer.Option(None, help="AWS region"),
    profile: Optional[str] = typer.Option(None, help="AWS profile"),
):
    """List recent runs for a Glue job."""
    if job_name:
        resolved_name = job_name
    else:
        stack = resolve_stack(None, region, profile)
        if module in JOB_NAME_TEMPLATES:
            resolved_name = JOB_NAME_TEMPLATES[module].format(stack=stack)
        else:
            resolved_name = module
    client = get_glue_client(region, profile)
    response = client.get_job_runs(JobName=resolved_name, MaxResults=limit)

    if output_json:
        runs_output = []
        for run in response.get("JobRuns", []):
            started = run.get("StartedOn")
            runs_output.append({
                "JobName": run.get("JobName", resolved_name),
                "RunId": run.get("Id", ""),
                "State": run.get("JobRunState", ""),
                "Started": started.isoformat() if started else None,
                "Duration": run.get("ExecutionTime", 0),
                "Workers": run.get("NumberOfWorkers"),
                "WorkerType": run.get("WorkerType", ""),
                "Arguments": run.get("Arguments", {}),
            })
        print(json.dumps(runs_output, indent=2))
        return

    table = Table(title=f"Recent Runs: {resolved_name}")
    table.add_column("Job Name", overflow="fold")
    table.add_column("Run ID", overflow="fold")
    table.add_column("State")
    table.add_column("Started")
    table.add_column("Duration")
    table.add_column("Workers")

    for run in response.get("JobRuns", []):
        started = run.get("StartedOn")
        started_str = started.strftime("%Y-%m-%d %H:%M") if started else ""
        duration = f"{run.get('ExecutionTime', 0)}s"
        table.add_row(
            run.get("JobName", resolved_name),
            run.get("Id", ""),
            run.get("JobRunState", ""),
            started_str,
            duration,
            str(run.get("NumberOfWorkers", "")),
        )

    wide_console = Console(width=max(console.width, 140))
    wide_console.print(table)


@app.command()
def jobs(
    prefix: str = typer.Option("AmazonKeyspaces", help="Filter jobs by name prefix"),
    region: Optional[str] = typer.Option(None, help="AWS region"),
    profile: Optional[str] = typer.Option(None, help="AWS profile"),
):
    """List all Keyspaces Glue jobs in the account."""
    client = get_glue_client(region, profile)
    response = client.get_jobs()

    table = Table(title="Keyspaces Glue Jobs")
    table.add_column("Job Name")
    table.add_column("Workers")
    table.add_column("Worker Type")
    table.add_column("Glue Version")

    for job in response.get("Jobs", []):
        if prefix and not job["Name"].startswith(prefix):
            continue
        table.add_row(
            job["Name"],
            str(job.get("NumberOfWorkers", "")),
            job.get("WorkerType", ""),
            job.get("GlueVersion", ""),
        )

    console.print(table)


# --- Job Commands ---


@app.command("export")
def run_export(
    keyspace: str = typer.Option(KEYSPACE_DEFAULT, help="Keyspace name"),
    table: str = typer.Option(TABLE_DEFAULT, help="Table name"),
    s3_uri: str = typer.Option(..., help="S3 URI for export output (e.g. s3://my-bucket)"),
    format: str = typer.Option(FORMAT_DEFAULT, help="Output format: parquet, json, csv"),
    driver_conf: str = typer.Option(DRIVER_CONF_DEFAULT, help="Driver config file name"),
    stack: Optional[str] = typer.Option(None, help="Stack name (auto-discovered if not set)"),
    job_name: Optional[str] = typer.Option(None, help="Override: full Glue job name"),
    workers: Optional[int] = typer.Option(None, help="Override number of workers"),
    region: Optional[str] = typer.Option(None, help="AWS region"),
    profile: Optional[str] = typer.Option(None, help="AWS profile"),
):
    """Export a Keyspaces table to S3."""
    resolved_stack = resolve_stack(stack, region, profile)
    resolved_name = resolve_job_name("export", job_name, resolved_stack)
    ensure_job_deployed("export", resolved_name, resolved_stack, region, profile)
    client = get_glue_client(region, profile)
    arguments = format_arguments({
        "KEYSPACE_NAME": keyspace,
        "TABLE_NAME": table,
        "S3_URI": s3_uri,
        "FORMAT": format,
        "DRIVER_CONF": driver_conf,
    })
    run_id = start_job_run(client, resolved_name, arguments, workers)
    console.print(f"[green]Started export job run:[/green] {run_id}")
    console.print(f"  Job: {resolved_name}")
    console.print(f"  Check status: keyspaces-bulk-cli status '{resolved_name}' '{run_id}'")


@app.command("import")
def run_import(
    keyspace: str = typer.Option(KEYSPACE_DEFAULT, help="Keyspace name"),
    table: str = typer.Option(TABLE_DEFAULT, help="Table name"),
    s3_uri: str = typer.Option(..., help="S3 URI to import from"),
    format: str = typer.Option(FORMAT_DEFAULT, help="Input format: parquet, json, csv"),
    driver_conf: str = typer.Option(DRIVER_CONF_DEFAULT, help="Driver config file name"),
    stack: Optional[str] = typer.Option(None, help="Stack name (auto-discovered if not set)"),
    job_name: Optional[str] = typer.Option(None, help="Override: full Glue job name"),
    workers: Optional[int] = typer.Option(None, help="Override number of workers"),
    region: Optional[str] = typer.Option(None, help="AWS region"),
    profile: Optional[str] = typer.Option(None, help="AWS profile"),
):
    """Import data from S3 into a Keyspaces table."""
    resolved_stack = resolve_stack(stack, region, profile)
    resolved_name = resolve_job_name("import", job_name, resolved_stack)
    ensure_job_deployed("import", resolved_name, resolved_stack, region, profile)
    client = get_glue_client(region, profile)
    arguments = format_arguments({
        "KEYSPACE_NAME": keyspace,
        "TABLE_NAME": table,
        "S3_URI": s3_uri,
        "FORMAT": format,
        "DRIVER_CONF": driver_conf,
    })
    run_id = start_job_run(client, resolved_name, arguments, workers)
    console.print(f"[green]Started import job run:[/green] {run_id}")
    console.print(f"  Job: {resolved_name}")
    console.print(f"  Check status: keyspaces-bulk-cli status '{resolved_name}' '{run_id}'")


@app.command("count")
def run_count(
    keyspace: str = typer.Option(KEYSPACE_DEFAULT, help="Keyspace name"),
    table: str = typer.Option(TABLE_DEFAULT, help="Table name"),
    distinct_keys: Optional[str] = typer.Option(None, help="Comma-separated columns for distinct count"),
    driver_conf: str = typer.Option(DRIVER_CONF_DEFAULT, help="Driver config file name"),
    stack: Optional[str] = typer.Option(None, help="Stack name (auto-discovered if not set)"),
    job_name: Optional[str] = typer.Option(None, help="Override: full Glue job name"),
    workers: Optional[int] = typer.Option(None, help="Override number of workers"),
    region: Optional[str] = typer.Option(None, help="AWS region"),
    profile: Optional[str] = typer.Option(None, help="AWS profile"),
):
    """Count rows in a Keyspaces table."""
    resolved_stack = resolve_stack(stack, region, profile)
    resolved_name = resolve_job_name("count", job_name, resolved_stack)
    ensure_job_deployed("count", resolved_name, resolved_stack, region, profile)
    client = get_glue_client(region, profile)
    arguments = format_arguments({
        "KEYSPACE_NAME": keyspace,
        "TABLE_NAME": table,
        "DISTINCT_KEYS": distinct_keys,
        "DRIVER_CONF": driver_conf,
    })
    run_id = start_job_run(client, resolved_name, arguments, workers)
    console.print(f"[green]Started count job run:[/green] {run_id}")
    console.print(f"  Job: {resolved_name}")
    console.print(f"  Check status: keyspaces-bulk-cli status '{resolved_name}' '{run_id}'")


@app.command("bulk-delete")
def run_bulk_delete(
    keyspace: str = typer.Option(KEYSPACE_DEFAULT, help="Keyspace name"),
    table: str = typer.Option(TABLE_DEFAULT, help="Table name"),
    distinct_keys: Optional[str] = typer.Option(None, help="Comma-separated primary key columns"),
    query_filter: Optional[str] = typer.Option(None, help="CQL filter to limit rows for deletion"),
    s3_uri: Optional[str] = typer.Option(None, help="S3 URI for backup before delete"),
    format: str = typer.Option(FORMAT_DEFAULT, help="Backup format: parquet, json, csv"),
    driver_conf: str = typer.Option(DRIVER_CONF_DEFAULT, help="Driver config file name"),
    stack: Optional[str] = typer.Option(None, help="Stack name (auto-discovered if not set)"),
    job_name: Optional[str] = typer.Option(None, help="Override: full Glue job name"),
    workers: Optional[int] = typer.Option(None, help="Override number of workers"),
    region: Optional[str] = typer.Option(None, help="AWS region"),
    profile: Optional[str] = typer.Option(None, help="AWS profile"),
):
    """Bulk delete rows from a Keyspaces table."""
    resolved_stack = resolve_stack(stack, region, profile)
    resolved_name = resolve_job_name("bulk-delete", job_name, resolved_stack)
    ensure_job_deployed("bulk-delete", resolved_name, resolved_stack, region, profile)
    client = get_glue_client(region, profile)
    arguments = format_arguments({
        "KEYSPACE_NAME": keyspace,
        "TABLE_NAME": table,
        "DISTINCT_KEYS": distinct_keys,
        "QUERY_FILTER": query_filter,
        "S3_URI": s3_uri,
        "FORMAT": format,
        "DRIVER_CONF": driver_conf,
    })
    run_id = start_job_run(client, resolved_name, arguments, workers)
    console.print(f"[green]Started bulk-delete job run:[/green] {run_id}")
    console.print(f"  Job: {resolved_name}")
    console.print(f"  Check status: keyspaces-bulk-cli status '{resolved_name}' '{run_id}'")


@app.command("modify-ttl")
def run_modify_ttl(
    keyspace: str = typer.Option(KEYSPACE_DEFAULT, help="Keyspace name"),
    table: str = typer.Option(TABLE_DEFAULT, help="Table name"),
    ttl_field: str = typer.Option(..., help="Column name that holds TTL value"),
    ttl_time_to_add: int = typer.Option(2592000, help="Seconds to add to TTL (negative to subtract, default 30 days)"),
    driver_conf: str = typer.Option(DRIVER_CONF_DEFAULT, help="Driver config file name"),
    stack: Optional[str] = typer.Option(None, help="Stack name (auto-discovered if not set)"),
    job_name: Optional[str] = typer.Option(None, help="Override: full Glue job name"),
    workers: Optional[int] = typer.Option(None, help="Override number of workers"),
    region: Optional[str] = typer.Option(None, help="AWS region"),
    profile: Optional[str] = typer.Option(None, help="AWS profile"),
):
    """Modify TTL values for rows in a Keyspaces table."""
    resolved_stack = resolve_stack(stack, region, profile)
    resolved_name = resolve_job_name("modify-ttl", job_name, resolved_stack)
    ensure_job_deployed("modify-ttl", resolved_name, resolved_stack, region, profile)
    client = get_glue_client(region, profile)
    arguments = format_arguments({
        "KEYSPACE_NAME": keyspace,
        "TABLE_NAME": table,
        "TTL_FIELD": ttl_field,
        "TTL_TIME_TO_ADD": str(ttl_time_to_add),
        "DRIVER_CONF": driver_conf,
    })
    run_id = start_job_run(client, resolved_name, arguments, workers)
    console.print(f"[green]Started modify-ttl job run:[/green] {run_id}")
    console.print(f"  Job: {resolved_name}")
    console.print(f"  Check status: keyspaces-bulk-cli status '{resolved_name}' '{run_id}'")


@app.command("incremental-export")
def run_incremental_export(
    keyspace: str = typer.Option(KEYSPACE_DEFAULT, help="Keyspace name"),
    table: str = typer.Option(TABLE_DEFAULT, help="Table name"),
    s3_uri: str = typer.Option(..., help="S3 URI for incremental diff output"),
    past_uri: str = typer.Option(..., help="S3 URI of the previous snapshot"),
    current_uri: str = typer.Option(..., help="S3 URI of the current snapshot"),
    distinct_keys: str = typer.Option(..., help="Comma-separated key columns for diff comparison"),
    format: str = typer.Option(FORMAT_DEFAULT, help="Format: parquet, json, csv"),
    driver_conf: str = typer.Option(DRIVER_CONF_DEFAULT, help="Driver config file name"),
    stack: Optional[str] = typer.Option(None, help="Stack name (auto-discovered if not set)"),
    job_name: Optional[str] = typer.Option(None, help="Override: full Glue job name"),
    workers: Optional[int] = typer.Option(None, help="Override number of workers"),
    region: Optional[str] = typer.Option(None, help="AWS region"),
    profile: Optional[str] = typer.Option(None, help="AWS profile"),
):
    """Compute incremental diff between two S3 snapshots."""
    resolved_stack = resolve_stack(stack, region, profile)
    resolved_name = resolve_job_name("incremental-export", job_name, resolved_stack)
    ensure_job_deployed("incremental-export", resolved_name, resolved_stack, region, profile)
    client = get_glue_client(region, profile)
    arguments = format_arguments({
        "KEYSPACE_NAME": keyspace,
        "TABLE_NAME": table,
        "S3_URI": s3_uri,
        "PAST_URI": past_uri,
        "CURRENT_URI": current_uri,
        "DISTINCT_KEYS": distinct_keys,
        "FORMAT": format,
        "DRIVER_CONF": driver_conf,
    })
    run_id = start_job_run(client, resolved_name, arguments, workers)
    console.print(f"[green]Started incremental-export job run:[/green] {run_id}")
    console.print(f"  Job: {resolved_name}")
    console.print(f"  Check status: keyspaces-bulk-cli status '{resolved_name}' '{run_id}'")


@app.command("incremental-import")
def run_incremental_import(
    keyspace: str = typer.Option(KEYSPACE_DEFAULT, help="Keyspace name"),
    table: str = typer.Option(TABLE_DEFAULT, help="Table name"),
    s3_uri: str = typer.Option(..., help="S3 URI of the incremental diff to import"),
    distinct_keys: str = typer.Option(..., help="Comma-separated key columns for delete operations"),
    format: str = typer.Option(FORMAT_DEFAULT, help="Format: parquet, json, csv"),
    driver_conf: str = typer.Option(DRIVER_CONF_DEFAULT, help="Driver config file name"),
    stack: Optional[str] = typer.Option(None, help="Stack name (auto-discovered if not set)"),
    job_name: Optional[str] = typer.Option(None, help="Override: full Glue job name"),
    workers: Optional[int] = typer.Option(None, help="Override number of workers"),
    region: Optional[str] = typer.Option(None, help="AWS region"),
    profile: Optional[str] = typer.Option(None, help="AWS profile"),
):
    """Import incremental diff (inserts, updates, deletes) into a Keyspaces table."""
    resolved_stack = resolve_stack(stack, region, profile)
    resolved_name = resolve_job_name("incremental-import", job_name, resolved_stack)
    ensure_job_deployed("incremental-import", resolved_name, resolved_stack, region, profile)
    client = get_glue_client(region, profile)
    arguments = format_arguments({
        "KEYSPACE_NAME": keyspace,
        "TABLE_NAME": table,
        "S3_URI": s3_uri,
        "DISTINCT_KEYS": distinct_keys,
        "FORMAT": format,
        "DRIVER_CONF": driver_conf,
    })
    run_id = start_job_run(client, resolved_name, arguments, workers)
    console.print(f"[green]Started incremental-import job run:[/green] {run_id}")
    console.print(f"  Job: {resolved_name}")
    console.print(f"  Check status: keyspaces-bulk-cli status '{resolved_name}' '{run_id}'")


@app.command("generate")
def run_generate(
    keyspace: str = typer.Option(KEYSPACE_DEFAULT, help="Keyspace name"),
    table: str = typer.Option(TABLE_DEFAULT, help="Table name"),
    driver_conf: str = typer.Option(DRIVER_CONF_DEFAULT, help="Driver config file name"),
    stack: Optional[str] = typer.Option(None, help="Stack name (auto-discovered if not set)"),
    job_name: Optional[str] = typer.Option(None, help="Override: full Glue job name"),
    workers: Optional[int] = typer.Option(None, help="Override number of workers"),
    region: Optional[str] = typer.Option(None, help="AWS region"),
    profile: Optional[str] = typer.Option(None, help="AWS profile"),
):
    """Generate random data into a Keyspaces table."""
    resolved_stack = resolve_stack(stack, region, profile)
    resolved_name = resolve_job_name("generate", job_name, resolved_stack)
    ensure_job_deployed("generate", resolved_name, resolved_stack, region, profile)
    client = get_glue_client(region, profile)
    arguments = format_arguments({
        "KEYSPACE_NAME": keyspace,
        "TABLE_NAME": table,
        "DRIVER_CONF": driver_conf,
    })
    run_id = start_job_run(client, resolved_name, arguments, workers)
    console.print(f"[green]Started generate job run:[/green] {run_id}")
    console.print(f"  Job: {resolved_name}")
    console.print(f"  Check status: keyspaces-bulk-cli status '{resolved_name}' '{run_id}'")


@app.command("compress-partition")
def run_compress_partition(
    keyspace: str = typer.Option(KEYSPACE_DEFAULT, help="Keyspace name"),
    source_table: str = typer.Option(..., help="Source table to read from"),
    target_table: str = typer.Option(..., help="Target table to write compressed data to"),
    compression: str = typer.Option("ZSTD", help="Compression algorithm: ZSTD, LZ4, SNAPPY, GZIP"),
    group_by_column: Optional[str] = typer.Option(None, help="Clustering column to group by (optional)"),
    driver_conf: str = typer.Option(DRIVER_CONF_DEFAULT, help="Driver config file name"),
    stack: Optional[str] = typer.Option(None, help="Stack name (auto-discovered if not set)"),
    job_name: Optional[str] = typer.Option(None, help="Override: full Glue job name"),
    workers: Optional[int] = typer.Option(None, help="Override number of workers"),
    region: Optional[str] = typer.Option(None, help="AWS region"),
    profile: Optional[str] = typer.Option(None, help="AWS profile"),
):
    """Compress partition data from source table into target table."""
    resolved_stack = resolve_stack(stack, region, profile)
    resolved_name = resolve_job_name("compress-partition", job_name, resolved_stack)
    ensure_job_deployed("compress-partition", resolved_name, resolved_stack, region, profile)
    client = get_glue_client(region, profile)
    arguments = format_arguments({
        "KEYSPACE_NAME": keyspace,
        "SOURCE_TABLE": source_table,
        "TARGET_TABLE": target_table,
        "COMPRESSION": compression,
        "GROUP_BY_COLUMN": group_by_column,
        "DRIVER_CONF": driver_conf,
    })
    run_id = start_job_run(client, resolved_name, arguments, workers)
    console.print(f"[green]Started compress-partition job run:[/green] {run_id}")
    console.print(f"  Job: {resolved_name}")
    console.print(f"  Check status: keyspaces-bulk-cli status '{resolved_name}' '{run_id}'")


@app.command("top-partitions")
def run_top_partitions(
    keyspace: str = typer.Option(KEYSPACE_DEFAULT, help="Keyspace name"),
    table: str = typer.Option(TABLE_DEFAULT, help="Table name"),
    group_by: str = typer.Option(..., help="Comma-separated columns to group by"),
    max_results: int = typer.Option(10, help="Number of top partitions to return"),
    min_size: int = typer.Option(1, help="Minimum partition size to include"),
    driver_conf: str = typer.Option(DRIVER_CONF_DEFAULT, help="Driver config file name"),
    stack: Optional[str] = typer.Option(None, help="Stack name (auto-discovered if not set)"),
    job_name: Optional[str] = typer.Option(None, help="Override: full Glue job name"),
    workers: Optional[int] = typer.Option(None, help="Override number of workers"),
    region: Optional[str] = typer.Option(None, help="AWS region"),
    profile: Optional[str] = typer.Option(None, help="AWS profile"),
):
    """Find the top-N largest partitions in a table."""
    resolved_stack = resolve_stack(stack, region, profile)
    resolved_name = resolve_job_name("top-partitions", job_name, resolved_stack)
    ensure_job_deployed("top-partitions", resolved_name, resolved_stack, region, profile)
    client = get_glue_client(region, profile)
    arguments = format_arguments({
        "KEYSPACE_NAME": keyspace,
        "TABLE_NAME": table,
        "GROUPBY": group_by,
        "MAX_RESULTS": str(max_results),
        "MIN_SIZE": str(min_size),
        "DRIVER_CONF": driver_conf,
    })
    run_id = start_job_run(client, resolved_name, arguments, workers)
    console.print(f"[green]Started top-partitions job run:[/green] {run_id}")
    console.print(f"  Job: {resolved_name}")
    console.print(f"  Check status: keyspaces-bulk-cli status '{resolved_name}' '{run_id}'")


if __name__ == "__main__":
    app()
