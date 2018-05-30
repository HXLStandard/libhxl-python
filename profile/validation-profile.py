import hxl

DATA_FILE='data/unhcr_popstats_export_persons_of_concern_all_data.hxl'

hxl.validate(hxl.io.make_input(DATA_FILE, allow_local=True))
