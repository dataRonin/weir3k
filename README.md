Weir3k
----


Essentially the same as `weir2k.py`, but now set up to run on Python 2 or Python 3, and with a few minor bug fixes in place. As we finish up this program new changes will be here, so that the version Don has and likes can stay intact in weir2k in case he needs it again, and this version with new features that may not complete before my termination will be available, but logically separate.



pyflow
----

`pyflow.py` is essentially the same as `pyFLOW.py` but with a few minor changes (none to math). It makes outputs of `_high`, `_daily`, `_montly`, and `_spoints`. `pyflow.py` ALWAYS reads from the `working` directory and always reads the `re` file.

1. It only uses numpy/scipy for doing interpolation, and otherwise does all the maths on its own. This keeps it from making a bunch of "nan" outputs in the statistics. This is good because those nans were corrupting the monthly outputs
2. It takes care of the issue where if the S-point were happening on a not-five-minute interval, it would stop the sampling because no match could be found. It moves all S-points to five minute intervals.
3. For MAINTEs, it marks the time stamp prior to the MAINTE with a MAINTV. For example, if the MAINTE is technically at 9:28, it will give a MAINTV to 9:25 and a MAINTE to 9:30.
4. If the data comes in with a crummy flag ("", for example, or ""M""), it will give it a more useful one, like "A" or "M".
5. If the data has a "nan" it will turn to a None, usually numerical.
6. The output for monthly now has a column for WATERYEAR and for ANNUAL YEAR as well as for MONTH.

