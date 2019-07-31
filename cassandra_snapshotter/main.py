from __future__ import (absolute_import, print_function)

# From system
from collections import defaultdict
from fabric.api import env
import os.path
import logging
import sys

# From package
from .snapshotting import (BackupWorker, RestoreWorker,
                           Snapshot, SnapshotCollection)
from .utils import add_s3_arguments
from .utils import base_parser as _base_parser

env.use_ssh_config = True


def run_backup(args):
    if args.user:
        env.user = args.user

    if args.password:
        env.password = args.password

    if args.sshkey:
        env.key_filename = args.sshkey

    if args.sshport:
        env.port = args.sshport

    env.hosts = args.hosts.split(',')
    env.keyspaces = args.keyspaces.split(',') if args.keyspaces else None

    if args.new_snapshot:
        create_snapshot = True
    else:
        existing_snapshot = SnapshotCollection(
            args.aws_access_key_id,
            args.aws_secret_access_key,
            args.s3_base_path,
            args.s3_bucket_name,
            args.s3_bucket_region
        ).get_snapshot_for(
            hosts=env.hosts,
            keyspaces=env.keyspaces,
            table=args.table
        )
        create_snapshot = existing_snapshot is None

    worker = BackupWorker(
        aws_access_key_id=args.aws_access_key_id,
        aws_secret_access_key=args.aws_secret_access_key,
        s3_bucket_region=args.s3_bucket_region,
        s3_ssenc=args.s3_ssenc,
        cassandra_conf_path=args.cassandra_conf_path,
        nodetool_path=args.nodetool_path,
        cassandra_bin_dir=args.cassandra_bin_dir,
        cqlsh_user=args.cqlsh_user,
        cqlsh_password=args.cqlsh_password,
        backup_schema=args.backup_schema,
        buffer_size=args.buffer_size,
        use_sudo=args.use_sudo,
        connection_pool_size=args.connection_pool_size,
        exclude_tables=args.exclude_tables,
        rate_limit=args.rate_limit,
        quiet=args.quiet
    )

    if create_snapshot:
        logging.info("Make a new snapshot")
        snapshot = Snapshot(
            base_path=args.s3_base_path,
            s3_bucket=args.s3_bucket_name,
            hosts=env.hosts,
            keyspaces=env.keyspaces,
            table=args.table
        )
        worker.snapshot(snapshot)
    else:
        logging.info("Add incrementals to snapshot {!s}".format(
            existing_snapshot))
        worker.update_snapshot(existing_snapshot)


def list_backups(args):
    snapshots = SnapshotCollection(
        args.aws_access_key_id,
        args.aws_secret_access_key,
        args.s3_base_path,
        args.s3_bucket_name,
        args.s3_bucket_region
    )
    path_snapshots = defaultdict(list)

    for snapshot in snapshots:
        dir_path = os.path.dirname(snapshot.base_path)
        path_snapshots[dir_path].append(snapshot)

    for path, snapshots in path_snapshots.iteritems():
        print("-----------[{!s}]-----------".format(path))
        for snapshot in snapshots:
            print("\t {!r} hosts:{!r} keyspaces:{!r} table:{!r}".format(
                snapshot, snapshot.hosts, snapshot.keyspaces, snapshot.table))
        print("------------------------{}".format('-' * len(path)))


def restore_backup(args):
    snapshots = SnapshotCollection(
        args.aws_access_key_id,
        args.aws_secret_access_key,
        args.s3_base_path,
        args.s3_bucket_name,
        args.s3_bucket_region,
    )
    if args.snapshot_name == 'LATEST':
        snapshot = snapshots.get_latest()
    else:
        snapshot = snapshots.get_snapshot_by_name(args.snapshot_name)

    worker = RestoreWorker(aws_access_key_id=args.aws_access_key_id,
                           aws_secret_access_key=args.aws_secret_access_key,
                           snapshot=snapshot,
                           cassandra_bin_dir=args.cassandra_bin_dir,
                           restore_dir=args.restore_dir,
                           no_sstableloader=args.no_sstableloader,
                           local_restore=args.local_restore,
                           s3_bucket_region=args.s3_bucket_region,
                           s3_bucket_name=args.s3_bucket_name)

    if args.hosts:
        hosts = args.hosts.split(',')

        if args.local_restore and len(hosts) > 1:
            logging.error(
                "You must provide only one source host when using --local.")
            sys.exit(1)
    else:
        hosts = snapshot.hosts

        if args.local_restore:
            logging.error(
                "You must provide one source host when using --local.")
            sys.exit(1)

    # --target_hosts is mutually exclusive with --local and --nosstableloader
    if args.target_hosts:
        target_hosts = args.target_hosts.split(',')
    else:
        # --local or --nosstableloader: no streaming will occur
        target_hosts = None

    worker.restore(args.keyspace, args.table, hosts, target_hosts)


def main():
    base_parser = add_s3_arguments(_base_parser)
    subparsers = base_parser.add_subparsers(
        title='subcommands', dest='subcommand'
    )

    subparsers.add_parser('list', help="List existing backups")

    backup_parser = subparsers.add_parser('backup', help="Create a snapshot")

    # snapshot / backup arguments
    backup_parser.add_argument(
        '--buffer-size',
        default=64,
        help="The buffer size (MB) for compress and upload")

    backup_parser.add_argument(
        '--exclude-tables',
        default='',
        help="Column families you want to skip")

    backup_parser.add_argument(
        '--hosts',
        required=True,
        help="Comma separated list of hosts to snapshot")

    backup_parser.add_argument(
        '--keyspaces',
        default='',
        help="Comma separated list of keyspaces to backup (omit to backup all)")

    backup_parser.add_argument(
        '--table',
        default='',
        help="The table (column family) to backup")

    backup_parser.add_argument(
        '--cassandra-conf-path',
        default='/etc/cassandra/conf/',
        help="cassandra config file path")

    backup_parser.add_argument(
        '--nodetool-path',
        default=None,
        help="nodetool path")

    backup_parser.add_argument(
        '--cassandra-bin-dir',
        default='/usr/bin',
        help="cassandra binaries directory")

    backup_parser.add_argument(
        '--user',
        help="The ssh user to logging on nodes")

    backup_parser.add_argument(
        '--use-sudo',
        default=False,
        help="Use sudo to run backup")

    backup_parser.add_argument(
        '--sshport',
        help="The ssh port to use to connect to the nodes")

    backup_parser.add_argument(
        '--password',
        default='',
        help="User password to connect with hosts")

    backup_parser.add_argument(
        '--sshkey',
        help="The file containing the private ssh key to use to connect with hosts")

    backup_parser.add_argument(
        '--new-snapshot',
        action='store_true',
        help="Create a new snapshot")

    backup_parser.add_argument(
        '--backup-schema',
        action='store_true',
        help="Backup (thrift) schema of selected keyspaces")

    backup_parser.add_argument(
        '--cqlsh-user',
        default='',
        help="User to use for cqlsh commands")

    backup_parser.add_argument(
        '--cqlsh-password',
        default='',
        help="Password to use for cqlsh commands")

    backup_parser.add_argument(
        '--connection-pool-size',
        default=12,
        help="Number of simultaneous connections to cassandra nodes")

    backup_parser.add_argument(
        '--rate-limit',
        default=0,
        help="Limit the upload speed to S3 (by using 'pv'). Value expressed in kilobytes (*1024)")

    backup_parser.add_argument(
        '--quiet',
        action='store_true',
        help="Set pv in quiet mode when using --rate-limit. "
             "Useful when called by a script.")

    # restore snapshot arguments
    restore_parser = subparsers.add_parser(
        'restore', help="Restores a snapshot")

    restore_parser.add_argument(
        '--snapshot-name',
        default='LATEST',
        help="The name (date/time) \
            of the snapshot (and incrementals) to restore")

    restore_parser.add_argument(
        '--keyspace',
        required=True,
        help="The keyspace to restore")

    restore_parser.add_argument(
        '--table',
        default='',
        help="The table (column family) to restore; leave blank for all")

    restore_parser.add_argument(
        '--hosts',
        default='',
        help="Comma separated list of hosts to restore from; "
             "leave empty for all. Only one host allowed when using --local.")

    restore_parser.add_argument(
        '--cassandra-bin-dir',
        default='/usr/bin',
        help="cassandra binaries directory")

    restore_parser.add_argument(
        '--restore-dir',
        default='/tmp/restore_cassandra/',
        help="Directory where data will be downloaded. "
             "Existing data in this directory will be *ERASED*. "
             "If --target-hosts is passed, sstableloader will stream data "
             "from this directory.")

    restore_type = restore_parser.add_mutually_exclusive_group(required=True)

    restore_type.add_argument(
        '--target-hosts',
        help='The comma separated list of hosts to restore into')

    restore_type.add_argument(
        '--local',
        action='store_true',
        dest='local_restore',
        help='Do not run sstableloader when restoring. If set, files will '
             'just be downloaded and decompressed in --restore-dir.')

    restore_type.add_argument(
        '--no-sstableloader',
        action='store_true',
        help="Do not run sstableloader when restoring. "
             "If set, files will just be downloaded. Use it if you want to do "
             "some checks and then run sstableloader manually.")

    args = base_parser.parse_args()
    subcommand = args.subcommand

    if args.verbose:
        logging.basicConfig(level=logging.INFO, format='%(message)s')

    if subcommand == 'backup':
        run_backup(args)
    elif subcommand == 'list':
        list_backups(args)
    elif subcommand == 'restore':
        restore_backup(args)


if __name__ == '__main__':
    main()
