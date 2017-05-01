import os
import argparse
import datetime
import logging
from utils import (
    get_db_connection,
    set_up_files_and_logger,
    get_nodes_to_export_from_db,
    write_html_content_to_output,
)

parser = argparse.ArgumentParser(
    description=(
        'ETL for getting drupal 7 post and user data and web site pages into '
        'JSON lines files'
    )
)
parser.add_argument(
    '--limit',
    help='INT: limit initial node query',
    dest='limit',
    type=int,
    required=False
)

parser.add_argument(
    '--dry-run',
    help=(
        'Flag to not save any data, just return printed info about what '
        'would happen'
    ),
    action='store_true',
    dest='dry_run',
)

parser.add_argument(
    '--epoch-changed',
    help=(
        'Specify an epoch timestamp to select as earliest \'changed\' value to'
        ' export data. Without this setting, the script '
        'will look for a stored file with the latest epoch of a previous run '
        'and default to 0.'
        'HINT: use --epoch-changed 1 to make the script select all posts. '
    ),
    dest='changed_epoch',
    type=int,
    required=False,
)

args = parser.parse_args()


def main():
    print "GIVE MY CREATION LIFE"
    logging_format = '%(levelname)-8s %(message)s'
    starttime = datetime.datetime.now()
    outfile_story, outfile_user, outfile_log_name = set_up_files_and_logger()
    logging.basicConfig(
        filename=outfile_log_name,
        level=logging.DEBUG,
        format=logging_format
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter(logging_format)
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

    logging.info("Logging to %s" % outfile_log_name)

    db = get_db_connection()
    if args.changed_epoch:
        highest_changed_epoch = int(args.changed_epoch)

    else:
        if os.path.exists('.highest_changed_epoch'):
            epoch_changed_file = open('.highest_changed_epoch', 'r')
            highest_changed_epoch = int(epoch_changed_file.read() or '0')
        else:
            highest_changed_epoch = 0

    nodes_to_export = get_nodes_to_export_from_db(
        highest_changed_epoch, db, args)

    if len(nodes_to_export) > 0:
        logging.info("Found %d stories to export" % len(nodes_to_export))
        if args.dry_run:
            return
        (
            story_success_count,
            user_success_count,
            error_count
        ) = write_html_content_to_output(
            db,
            nodes_to_export,
            outfile_story,
            outfile_user,
        )

        outfile_story.close()
        outfile_user.close()

        logging.info("Parsed %d stories" % story_success_count)
        logging.info("Parsed %d users" % user_success_count)
        logging.info("Had %d errors" % error_count)
    else:
        logging.info("Didn't find anything to export")
    endtime = datetime.datetime.now()
    total_time = endtime - starttime
    logging.info("total time: %s" % total_time)

if __name__ == "__main__":
    main()
