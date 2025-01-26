import json
import os

# get the path of this module, go up one directory and add config.json to the path
CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "config.json")


class ConfigObj:
    """class to hold one layer of the bot configuration. It's elements may be other ConfigObj's
    """

    def __init__(self, **kwargs):
        self.__dict__.update({k: ConfigObj(**v) if isinstance(v, dict) else v
                              for k, v in kwargs.items()})

    def __getitem__(self, item):
        """adds support for ConfigObj.element as ConfigObj['element']
        """
        return self.__dict__[item]

    def get(self, key: str):
        """alias function to mask ConfigObj.element as ConfigObj.get('element')
        """

        return self.__dict__[key]
    
    def add(self, key: str, value):
        """add a key-value pair to the ConfigObj
        Parameters
        ----------
            key: string
                the key to add
            value: any
                the value to add
        """
        new = {key: value if not isinstance(value, (ConfigObj, dict)) else ConfigObj(**value)}
        self.__dict__.update(new)

    def to_dict(self) -> dict:
        """recursively transform this class and all sub-classes to a dictionary
        Returns
        -------
            dict
        """

        return {k: v.to_dict() if isinstance(v, ConfigObj) else v
                for k, v in self.__dict__.items()}


class Config:
    """class to represent the bot configuration
    Methods
    -------
        load:
            read a json file and recursively represent it's entries as class attributes
    """

    def __init__(self):
        pass

    def load(self, path: str = CONFIG_PATH) -> None:
        """read a json file and recursively represent it's entries as class attributes
        Parameters
        ----------
            path: string
                the path to the json file
        """

        # read the json file
        with open(path) as fp:
            d = json.load(fp)

        # convert json to class attributes
        d = {k: ConfigObj(**v) if isinstance(v, dict) else v for k, v in d.items()}

        # clear all existing config and update with the new data. This allows for mid-run updates
        # by re-running the .load-method
        self.__dict__.clear()
        self.__dict__.update(d)

    def to_json(self, path: str = CONFIG_PATH) -> None:
        """write the current config state to a json file, overwriting the path
        Parameters
        ----------
            path: string
                the path to the json file
        """

        d = {k: v.to_dict() if isinstance(v, ConfigObj) else v
             for k, v in self.__dict__.items()}

        with open(path, 'w+') as outfile:
            json.dump(d, outfile, indent=2)

