from typing import Optional
import abc
import asyncio


class AsyncTask(object, metaclass=abc.ABCMeta):
    """
    Async task represents a task to be completed asynchronously
    """

    @abc.abstractclassmethod
    async def execute(self) -> Optional[object]:
        """method to execute async task"""
        pass

    def execute_sync(self) -> Optional[object]:
        """method to execute task synchronously"""
        return asyncio.run(self.execute())


class AsyncExecutionError(Exception):
    """
    AsyncExecutionError is thrown when unable to execute async
    execution method

    Parameters
    ----------
    msg: str
        message to log for the error
    """

    def __init__(self, msg: str) -> None:
        self.message = msg
        super().__init__(self.message)

    def get_message(self) -> str:
        """method to get error message"""
        return f'AsyncExecutionError-> {self.message}'
