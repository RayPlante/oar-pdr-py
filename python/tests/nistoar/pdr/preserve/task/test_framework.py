import os, json, pdb, logging, tempfile
from pathlib import Path
import unittest as test

from nistoar.pdr.preserve.task import framework as fw

class TestPreservationStepAware(test.TestCase, fw.PreservationStepsAware):

    def test_last_step_in(self):
        self.assertEqual(self._last_step_in(64),  self.PUBLISHED)
        self.assertEqual(self._last_step_in(self.ARCHIVED),  self.ARCHIVED)
        self.assertEqual(self._last_step_in(0),  self.UNSTARTED)
        self.assertEqual(self._last_step_in(13), self.SERIALIZED)
        self.assertEqual(self._last_step_in(self.FINALIZED|self.SERIALIZED), self.SERIALIZED)

    def test_label_for_step(self):
        self.assertEqual(self._label_for_step(0), "unstarted")
        self.assertEqual(self._label_for_step(self.UNSTARTED), "unstarted")
        self.assertEqual(self._label_for_step(self.FINALIZED), "finalized")
        self.assertEqual(self._label_for_step(self.VALIDATED), "validated")
        self.assertEqual(self._label_for_step(self.SERIALIZED), "serialized")
        self.assertEqual(self._label_for_step(self.SUBMITTED), "submitted to archive")
        self.assertEqual(self._label_for_step(self.ARCHIVED), "archived")
        self.assertEqual(self._label_for_step(self.PUBLISHED), "published")
        self.assertEqual(self._label_for_step(49), "archived")
        
        





                         
if __name__ == '__main__':
    test.main()




