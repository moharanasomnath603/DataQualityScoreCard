/*
 * Copyright (c) 2018, Anthem Inc. All rights reserved.
 * DO NOT ALTER OR REMOVE THIS FILE HEADER.
 *
 */

package dqs.ccdri.workflow.rules

/**
 *
 * @author Somanatha Moharana
 * @version  1.0
 * 
 * A generic interface for Extract Transform Load implementations
 */

trait ExecuteRulesTrait {

   /**
   * A generic method for importing Data from sources
   */
  def executeRules () {}

   /**
   * A generic method for importing Data from sources with args
   */
  def executeRules (args:String) {}
}