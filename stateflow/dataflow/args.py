from typing import Dict, Any, List, Optional


class Arguments:

    __slots__ = "_args"

    def __init__(self, args: Dict[str, Any]):
        self._args: Dict[str, Any] = args

    def __getitem__(self, item):
        return self._args[item]

    def __setitem__(self, key, value):
        self._args[key] = value

    def get(self) -> Dict[str, Any]:
        return self._args

    def get_keys(self) -> List[str]:
        return list(self._args.keys())

    def toDict(self):
        """For custom ujson serialization"""
        return self.to_dict()

    def to_dict(self) -> Dict:
        return self._args

    @staticmethod
    def from_dict(dictionary: Dict):
        return Arguments(dictionary)

    @staticmethod
    def from_args_and_kwargs(
        desc: Dict[str, Any], *args, **kwargs
    ) -> Optional["Arguments"]:
        args_dict = {}
        for arg, name in zip(list(args), desc.keys()):
            args_dict[name] = arg

        for key, value in kwargs.items():
            args_dict[key] = value

        arguments = Arguments(args_dict)

        if not desc.keys() == args_dict.keys():
            raise AttributeError(f"Expected arguments: {desc} but got {args_dict}.")

        return arguments
