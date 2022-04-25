class Date:
	LUT = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
	def __init__(self, d, m, y):
		self.d = d
		self.m = m
		self.y = y
	
	def __str__(self):
		d = str(self.d)
		m = str(self.m)
		y = str(self.y)
		return y + "-" + ((2 - len(m)) * "0") + m + "-" + ((2 - len(d)) * "0") + d

	def __eq__(self, value):
		return (self.d == value.d and self.m == value.m and self.y == value.y)

	def __add__(self, x):
		new = Date(self.d, self.m, self.y)
		while x:
			new.d = new.d + 1
			if (new.d > new.LUT[new.m - 1]) and (new.m != 2 or new.y % 4 != 0 or new.d != 29):
				new.d = 1
				new.m = new.m + 1
				if new.m > 12:
					new.m = 1
					new.y = new.y + 1
			x = x - 1
		return new

	def __sub__(self, x):
		new = Date(self.d, self.m, self.y)
		while x:
			new.d = new.d - 1
			if new.d == 0:
				new.m = new.m - 1
				if new.m == 0:
					new.y = new.y - 1
					new.m = 12
				new.d = new.LUT[new.m - 1]
				if new.m == 2 and new.y % 4 == 0:
					new.d = new.d + 1
			x = x - 1
		return new