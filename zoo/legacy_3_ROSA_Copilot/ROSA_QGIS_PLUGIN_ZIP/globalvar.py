class globalvar:
    @staticmethod
    def _init():
        global _global_dict
        _global_dict = {}

    @staticmethod
    def set_value(name, value):
        _global_dict[name] = value

    @staticmethod
    def get_value(name, defValue=None):
        try:
            return _global_dict[name]
        except KeyError:
            return defValue


# def _init():
#     global _global_dict
#     _global_dict = {}

# def set_value(name, value):
#     _global_dict[name] = value

# def get_value(name, defValue=None):
#     try:
#         return _global_dict[name]
#     except KeyError:
#         return defValue