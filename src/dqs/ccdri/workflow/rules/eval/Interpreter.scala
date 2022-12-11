package dqs.ccdri.workflow.rules.eval

import java.util.Stack

/**
 * Receives expression to be evaluated
 * @author Somnath
 */

object Interpreter {

	private var stack = new Stack[String]
	private var resultantStack = new Stack[String]
	
	def push(str:String) = stack.push(str)

	def pop = if (stack.isEmpty()) null else stack.pop()

	def pushRStack(str:String) = resultantStack.push(str)

	def popRStack = if (resultantStack.isEmpty()) null else resultantStack.pop()

	def isDelimiter(c:Char) = "()*/-+,".indexOf(c) > -1

	private def popTillRightBracket = {
		var item = popRStack
		var expression = ""
		while (item != null && !item.equalsIgnoreCase(")")) {
			expression += item
			item = popRStack
		}
		expression
	}

	private def iterateStack = {
		var item = pop
		while (item != null) {
			pushRStack(item)
			if (item.equals("(")) {
				popRStack
				val nextItem = pop
				val ef = (nextItem.toLowerCase)
				if (ef != null) {
					push(popTillRightBracket)
				} else {
					push(nextItem)
				}
			}

			item = pop
		}
		System.out.println("Output = " + popRStack)

	}
	
	def evaluate(str1:String) = {
		val str = str1.replaceAll("\"", "")
		val expressions = str.toCharArray()
		var temp = ""
		for (exp <- expressions) {
			if (isDelimiter(exp)) {
				push(temp)
				push("" + exp)
				temp = ""
			} else {
				temp += exp
			}
		}
		System.out.println("*********************************************")
		System.out.println("Input  = " + str)
		iterateStack
		System.out.println("*********************************************")
	}
}
