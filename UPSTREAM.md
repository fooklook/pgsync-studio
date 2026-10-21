# Upstream attribution

PGSync Studio includes and extends code from
[toluaina/pgsync](https://github.com/toluaina/pgsync).

The included upstream package identifies itself as PGSync 7.0.5. The independent
Studio distribution has its own version, `0.1.0`, in `pgsync.web.__version__`.
It retains the `pgsync` import namespace for compatibility with the engine.

The original MIT license and `Copyright (c) 2023 Tolu Aina` notice are retained
unchanged in [LICENSE](LICENSE). Upstream contributor credits are retained in
[AUTHORS.rst](AUTHORS.rst).

Studio extensions include the Web UI, document assembly, cross-database runners,
external configuration storage, task coordination, replica read fencing, durable
events, recovery, metrics, and partition maintenance. This project is independently
maintained and is not endorsed by or published on behalf of the upstream author.

This public snapshot starts a new Git repository. It does not import the history
of a private deployment repository. Public installation metadata deliberately
does not point Studio issue reporting or package publication at upstream accounts.
