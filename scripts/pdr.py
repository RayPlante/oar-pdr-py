#!/usr/bin/env python3
#
import os, sys
import traceback as tb

from nistoar.pdr import cli, def_etc_dir
from nistoar.pdr.exceptions import ConfigurationException
# import nistoar.pdr.publish.cmd as pub
# import nistoar.pdr.preserv.cmd as preserve
from nistoar.pdr.cli import md

def main(cmdname, args):

    # set up the commands
    pdr = cli.PDRCLI(cmdname, os.path.join(def_etc_dir, "pdr-cli-config.yml"))
#    pdr.load_subcommand(pub)
#    pdr.load_subcommand(preserve)
    pdr.load_subcommand(md)

    # execute the commands
    args = pdr.parse_args(args)
    pdr.execute(args)
    return args

if __name__ == "__main__":
    args = None
    try:
        prog = os.path.splitext(os.path.basename(sys.argv[0]))[0]
        args = main(prog, sys.argv[1:])
        sys.exit(0)
    except cli.PDRCommandFailure as ex:
        if not args or not args.quiet:
            print(prog, ex.cmd+":", str(ex), file=sys.stderr)
        sys.exit(ex.stat)
    except ConfigurationException as ex:
        if not args or not args.quiet:
            print(prog, ex.cmd+": config error: ", str(ex), file=sys.stderr)
        sys.exit(4)
    except Exception as ex:
        if not args or not args.quiet:
            tb.print_exc(file=sys.stderr)
            print(prog+":", repr(ex), file=sys.stderr)
        sys.exit(200)


        


