from premsql.playground import AgentServer
from premsql.agents import BaseLineAgent
from premsql.generators import Text2SQLGeneratorPremAI
from premsql.executors import ExecutorUsingLangChain
from premsql.agents.tools import SimpleMatplotlibTool

# Initialize components
generator = Text2SQLGeneratorPremAI()
executor = ExecutorUsingLangChain()
agent = BaseLineAgent(generator=generator, executor=executor, tools=[SimpleMatplotlibTool()])


natural_language_query = "List all employees who joined after 2020."
sql_query = agent.generate_sql(natural_language_query)
print("Generated SQL Query:", sql_query)
