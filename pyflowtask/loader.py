#!/usr/bin/env python
# -*- coding: utf-8 -*-

import abc


class FlowLoader(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def load(self, *a, **ka):
        pass


class ModuleFlowLoader(FlowLoader):
    """File based task flows loader
    """

    def load(self, *a, **ka):
        pass
        