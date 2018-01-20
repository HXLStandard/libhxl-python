import hxl

DATA = [
    ['#affected'],
    [200]
]

print(hxl.data(DATA).clean_data(number='affected', number_format='0.2f').values)
