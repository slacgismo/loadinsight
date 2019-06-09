"""Check implements all the assertions needed to verify data"""
def assertall(x):
	"""Assert truth of all members of a list (i.e., 'and')"""
	assert(x.all())

def assertany(x):
	"""Assert truth of any member of a list (i.e., 'or')"""
	assert(x.any())

def normal_max_rows(data):
    assertall( data.min() == 0.0 )
    assertall( data.max() == 1.0 )

def normal_sum_rows(data):
    assertall( (data.sum()-1.0).abs() < 0.0001 )

def random(data):
    assertall( data.mean() <= data.max() )
    assertall( data.mean() >= data.min() )
    assertall( data.std() <= (data.max()-data.min()) )
