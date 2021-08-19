import ast

def parse_math(expression):
    """
    A function I got off stackoverflow that enables python to parse user input as a mathematical expression.
    probably a huge security risk. but it enables the user to enter missing characterization values during runtime.
    :param expression:
    :return:
    """
    try:
        tree = ast.parse(expression, mode='eval')
    except SyntaxError:
        return    # not a Python expression
    if not all(isinstance(node, (ast.Expression, ast.UnaryOp, ast.unaryop, ast.BinOp, ast.operator, ast.Num))
               for node in ast.walk(tree)):
        return    # not a mathematical expression (numbers and operators)
    return eval(compile(tree, filename='', mode='eval'))
