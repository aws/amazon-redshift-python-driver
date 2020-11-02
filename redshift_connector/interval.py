# from redshift_connector.config import max_int4, max_int8, min_int4, min_int8
#
#
# class Interval:
#     """An Interval represents a measurement of time.  In PostgreSQL, an
#     interval is defined in the measure of months, days, and microseconds; as
#     such, the interval type represents the same information.
#
#     Note that values of the :attr:`microseconds`, :attr:`days` and
#     :attr:`months` properties are independently measured and cannot be
#     converted to each other.  A month may be 28, 29, 30, or 31 days, and a day
#     may occasionally be lengthened slightly by a leap second.
#
#     .. attribute:: microseconds
#
#         Measure of microseconds in the interval.
#
#         The microseconds value is constrained to fit into a signed 64-bit
#         integer.  Any attempt to set a value too large or too small will result
#         in an OverflowError being raised.
#
#     .. attribute:: days
#
#         Measure of days in the interval.
#
#         The days value is constrained to fit into a signed 32-bit integer.
#         Any attempt to set a value too large or too small will result in an
#         OverflowError being raised.
#
#     .. attribute:: months
#
#         Measure of months in the interval.
#
#         The months value is constrained to fit into a signed 32-bit integer.
#         Any attempt to set a value too large or too small will result in an
#         OverflowError being raised.
#     """
#
#     def __init__(self: 'Interval', microseconds: int = 0, days: int = 0, months: int = 0) -> None:
#         self.microseconds = microseconds
#         self.days = days
#         self.months = months
#
#     def _setMicroseconds(self: 'Interval', value: int) -> None:
#         if not isinstance(value, int):
#             raise TypeError("microseconds must be an integer type")
#         elif not (min_int8 < value < max_int8):
#             raise OverflowError(
#                 "microseconds must be representable as a 64-bit integer")
#         else:
#             self._microseconds = value
#
#     def _setDays(self: 'Interval', value: int) -> None:
#         if not isinstance(value, int):
#             raise TypeError("days must be an integer type")
#         elif not (min_int4 < value < max_int4):
#             raise OverflowError(
#                 "days must be representable as a 32-bit integer")
#         else:
#             self._days = value
#
#     def _setMonths(self: 'Interval', value: int) -> None:
#         if not isinstance(value, int):
#             raise TypeError("months must be an integer type")
#         elif not (min_int4 < value < max_int4):
#             raise OverflowError(
#                 "months must be representable as a 32-bit integer")
#         else:
#             self._months = value
#
#     microseconds = property(lambda self: self._microseconds, _setMicroseconds)
#     days = property(lambda self: self._days, _setDays)
#     months = property(lambda self: self._months, _setMonths)
#
#     def __repr__(self: 'Interval') -> str:
#         return "<Interval %s months %s days %s microseconds>" % (
#             self.months, self.days, self.microseconds)
#
#     def __eq__(self: 'Interval', other: object) -> bool:
#         return other is not None and isinstance(other, Interval) and \
#             self.months == other.months and self.days == other.days and \
#             self.microseconds == other.microseconds
#
#     def __neq__(self: 'Interval', other: 'Interval') -> bool:
#         return not self.__eq__(other)
