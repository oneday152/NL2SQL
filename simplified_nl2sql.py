from abc import ABC, abstractmethod
from typing import List, Dict, Any, Callable, Type, Optional
from dataclasses import dataclass
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage
import json
import dotenv
import os
import sqlite3
import time
import re
from pydantic import BaseModel
from typing import Literal
from collections import defaultdict

from prompt_all import *
from databasemanager import DatabaseManager

@dataclass
class AgentContext:
    question: str
    hint: str
    db_name: str
    intermediate_results: Dict[str, Any]

class ReasoningStep(BaseModel):
    title: str
    content: str
    next_action: Literal["continue", "final_answer"]
    final_sql: Optional[str] = None

class FinalAnswer(BaseModel):
    title: str
    content: str

class Node(ABC):
    def __init__(self, name: str):
        self.name = name
    
    @abstractmethod
    def process(self, context: AgentContext) -> Dict[str, Any]:
        pass

class AgentNode(Node):
    def __init__(self, name: str):
        super().__init__(name)
        self.chat_model = ChatOpenAI(
            openai_api_base=os.getenv("OPENAI_API_BASE"),
            model='gpt-4o',
            temperature=0.6
        )
        self.system_prompt = ""

def make_api_call(messages, max_tokens, is_final_answer=False):
    for attempt in range(3):
        try:
            format_schema = ReasoningStep if not is_final_answer else FinalAnswer
            chat_model = ChatOpenAI(
                openai_api_base=os.getenv("OPENAI_API_BASE"),
                model='gpt-4o',
                temperature=0.6
            )
            
            messages[0]["content"] += f"\nOutput must strictly follow this JSON schema:\n{json.dumps(format_schema.model_json_schema(), indent=2)}"
            
            formatted_messages = []
            for msg in messages:
                if msg["role"] == "system":
                    formatted_messages.append(SystemMessage(content=msg["content"]))
                elif msg["role"] == "user":
                    formatted_messages.append(HumanMessage(content=msg["content"]))
                elif msg["role"] == "assistant":
                    formatted_messages.append(AIMessage(content=msg["content"]))
            
            response = chat_model.invoke(formatted_messages)
            
    
            try:
                content = response.content
                if isinstance(content, str):
                    content = content.replace('```json', '').replace('```', '').strip()
                    start = content.find('{')
                    end = content.rfind('}') + 1
                    if start != -1 and end != 0:
                        content = content[start:end]
                
                return format_schema.model_validate_json(content)
            except Exception as json_error:
                print(f"JSON parsing error: {str(json_error)}")
                print(f"Raw content: {content}")
                raise
                
        except Exception as e:
            if attempt == 2:
                if is_final_answer:
                    return FinalAnswer(title="Error", content=f"Failed to generate final answer after 3 attempts. Error: {str(e)}")
                else:
                    return ReasoningStep(title="Error", 
                                       content=f"Failed to generate step after 3 attempts. Error: {str(e)}", 
                                       next_action="final_answer")
            time.sleep(1)

# o1-like思维链生成
def generate_o1_reasoning(prompt):
    messages = [
        {"role": "system", "content": """You are an expert SQL designer that explains your reasoning step by step. For each step, provide a title that describes what you're doing in that step, along with the content. Decide if you need another step or if you're ready to give the final answer. 

When your next_action is "final_answer", you MUST include a "final_sql" field with the complete SQL query.

Take one step of reasoning at a time.Respond in JSON format with 'title', 'content', 'next_action' (either 'continue' or 'final_answer') keys, and 'final_sql' when providing the final answer. 

In all your reasoning steps, at least one step must involve revising the important rules. During this step, you need to check each rule individually to ensure that the SQL complies with the rules.

USE AS MANY REASONING STEPS AS POSSIBLE. AT LEAST 3. BE AWARE OF YOUR LIMITATIONS AS AN LLM AND WHAT YOU CAN AND CANNOT DO. IN YOUR REASONING, INCLUDE EXPLORATION OF ALTERNATIVE SQL DESIGNS. CONSIDER YOU MAY BE WRONG, AND IF YOU ARE WRONG IN YOUR REASONING, WHERE IT WOULD BE. FULLY TEST ALL OTHER POSSIBILITIES. YOU CAN BE WRONG. WHEN YOU SAY YOU ARE RE-EXAMINING, ACTUALLY RE-EXAMINE, AND USE ANOTHER APPROACH TO DO SO. DO NOT JUST SAY YOU ARE RE-EXAMINING. USE AT LEAST 3 METHODS TO DERIVE THE ANSWER. USE BEST PRACTICES.

IMPORTANT SQL DESIGN GUIDELINES:
1. When finding maximum or minimum values based on specific conditions, use ORDER BY + LIMIT 1 instead of MAX/MIN in subqueries.
2. Pay attention to the data storage format when sorting, comparing sizes, or performing calculations. If the data is in string format, process it using INSTR or SUBSTR before comparison.
3. If your query includes an ORDER BY clause to sort results, only include columns used for sorting in the SELECT clause if specifically requested in the question. Otherwise, omit these columns from the SELECT clause.
4. Ensure you only output information requested in the question. If the question asks for specific columns, ensure the SELECT clause only includes those columns, nothing more.
5. The query should return all information requested in the question - no more, no less.
6. For key phrases mentioned in the question, we have marked the most similar values with "EXAMPLE" in front of the corresponding column names. This is an important hint indicating the correct columns to use for SQL queries.
7. NEVER use || ' ' || for string concatenation. This is strictly prohibited and will result in severe penalties.

Example of a valid JSON response for a continuing step:
```json
{
    "title": "Identifying Key Tables and Columns",
    "content": "To begin solving this SQL problem, we need to carefully examine the given information and identify the crucial tables and columns that will guide our solution process. This involves...",
    "next_action": "continue"
}```

Example of a valid JSON response for the final step:
```json
{
    "title": "Finalizing the SQL Query",
    "content": "After careful analysis and consideration of all alternatives, I've determined the optimal SQL query for this problem...",
    "next_action": "final_answer",
    "final_sql": "SELECT column1, column2 FROM table1 JOIN table2 ON table1.id = table2.id WHERE condition = 'value';"
}```
Your response must be in JSON format!!
"""},
        {"role": "user", "content": prompt},
        {"role": "assistant", "content": "Thank you! I will now think step by step following my instructions, starting at the beginning after decomposing the problem."}
    ]
    
    
    steps = []
    step_count = 1
    
    while True:
        print(f"\n===== COT Step {step_count} =====")
        step_data = make_api_call(messages, 300)
        steps.append(step_data)
        
        print(f"Title: {step_data.title}")
        print(f"Content: {step_data.content[:200]}..." if len(step_data.content) > 200 else f"内容: {step_data.content}")
        print(f"Next step: {step_data.next_action}")
        if step_data.final_sql:
            print(f"Final SQL: {step_data.final_sql}")
        
        messages.append({"role": "assistant", "content": step_data.model_dump_json()})
        
        if step_data.next_action == 'final_answer' and hasattr(step_data, 'final_sql') and step_data.final_sql:
            return f"FINAL SQL: {step_data.final_sql}"
        
        if step_data.next_action == 'final_answer' or step_count > 10: 
            break
        
        step_count += 1
    
    print("\n===== Generate Final Answer =====")
    messages.append({"role": "user", "content": "Please provide the final answer based on your reasoning above. Make sure to include the final SQL query in the 'final_sql' field."})
    final_data = make_api_call(messages, 200, is_final_answer=True)
    
  
    print(f"最终答案标题: {final_data.title}")
    print(f"最终答案内容: {final_data.content[:200]}..." if len(final_data.content) > 200 else f"最终答案内容: {final_data.content}")
    
  
    if hasattr(final_data, 'final_sql') and final_data.final_sql:
        print(f"FINAL SQL: {final_data.final_sql}")
        return f"FINAL SQL: {final_data.final_sql}"
    
    return final_data.content

class TableAgent(AgentNode):
    def __init__(self):
        super().__init__("table_selector")
        self.chat_model = ChatOpenAI(
            openai_api_base=os.getenv("OPENAI_API_BASE"),
            model='gpt-4o',
            temperature=0
        )
        self.system_prompt = TABLE_SELECTOR_PROMPT
    
    def process(self, context: AgentContext) -> Dict[str, Any]:
        try:
            if "db_schema" in context.intermediate_results:
                all_tables_columns = context.intermediate_results["db_schema"]
            else:
                db_manager = DatabaseManager(context.db_name)
                all_tables_columns = db_manager.get_table_columns_dict()

            table_column_description = self._get_table_column_descriptions(
                all_tables_columns, 
                context.db_name
            )
            
            query = f'''################################################################################################################################################################################################
QUESTION: {context.question}
HINT: {context.hint}
DATABASE SCHEMA: {json.dumps(all_tables_columns, ensure_ascii=False, indent=4)}
COLUMN DESCRIPTION: {json.dumps(table_column_description, ensure_ascii=False, indent=4)}

This schema provides a detailed definition of the database's structure, including tables, their columns, and any relevant details about relationships or constraints.  

Now follow the instructions and the example to select the relevant tables and columns:
Let's think step by step as the example:  
1. Understand the question and hint.  
2. Examine the available tables and columns.  
3. Prioritize the tables and columns explicitly mentioned in the HINT.  
4. Use column descriptions to verify additional relevance, but only after confirming the HINT's tables and columns are selected.  
5. Generate output.

---

### Important Notes:
1. **Priority Selection**:  
   - **You MUST prioritize the tables and columns explicitly mentioned in the HINT**: `{context.hint}`. These tables and columns must be selected directly. Avoid using similar ones unless explicitly permitted, as this will lead to errors.  

2. **All Relevant Columns**:  
   - Select all possible relevant columns from the tables mentioned in the HINT, not just the most relevant ones. Missing any column explicitly listed or related to the HINT will lead to errors.  

3. **Avoid Similar Tables and Columns**:  
   - Do not infer or select tables or columns that appear similar to those in the HINT but are not explicitly listed. This rule ensures accuracy and avoids logical errors in the SQL query.  

4. **Output Format**:  
   - Ensure that the final output is presented in JSON format as shown below.  

---

### OUTPUT FORMAT:
```json
{{
    "table1": ["column1", "column2", "..."],
    "table2": ["column3", "column4", "..."]
}}
################################################################################################################################################################################################
'''
            
            result = self.chat_model.invoke([
                SystemMessage(content=self.system_prompt),
                HumanMessage(content=query)
            ])
            print(result.content)
            selected_tables = self._parse_json_output(result.content)
            return {"selected_tables": selected_tables}
        except Exception as e:
            print(f"TableAgent error: {str(e)}")
            raise
            
    def _get_table_column_descriptions(self, all_tables_columns: Dict[str, List[str]], db_name: str) -> Dict[str, Dict]:
        import pandas as pd
        table_column_description = {}
        base_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", db_name)
        
        encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']

        for table_name, column_names in all_tables_columns.items():
            table_path = os.path.join(base_path, "database_description", f"{table_name}.csv")
            table_description = None
            
            for encoding in encodings:
                try:
                    table_description = pd.read_csv(table_path, encoding=encoding)
                    break  
                except UnicodeDecodeError:
                    continue  
                except Exception as e:
                    print(f"读取表{table_name}时发生非编码错误: {str(e)}")
                    break
                
            if table_description is None:
                print(f"表{table_name}的描述文件无法使用任何编码读取")
                table_column_description[table_name] = {}
                continue
            
            try:
                column_description = {}
                
                for column_name in column_names:
                    result = table_description[
                        table_description['original_column_name'].str.contains(
                            column_name, case=False, na=False, regex=False
                        )
                    ]
                    
                    if not result.empty:
                        column_info = {
                            "column_description": result.iloc[0].get('column_description', ''),
                            "value_description": result.iloc[0].get('value_description', '')
                        }
                        column_description[column_name] = column_info
                    else:
                        column_description[column_name] = {
                            "column_description": "",
                            "value_description": ""
                        }
                
                table_column_description[table_name] = column_description
            except Exception as e:
                print(f"处理表 {table_name} 描述时出错: {str(e)}")
                table_column_description[table_name] = {}
            
        return table_column_description
            
    def _parse_json_output(self, content: str) -> Dict[str, List[str]]:
        output_text = content.strip()
        
        try:
            if output_text.startswith('{') and output_text.endswith('}'):
                return json.loads(output_text)
            
            if '```json' in output_text:
                pattern = r'```json\s*(.*?)\s*```'
                matches = list(re.finditer(pattern, output_text, re.DOTALL))
                if matches:
                    json_str = matches[-1].group(1).strip()
                    return json.loads(json_str)
            
            dict_pattern = r'{([^}]+)}'
            match = re.search(dict_pattern, output_text)
            if match:
                dict_str = '{' + match.group(1) + '}'
                dict_str = re.sub(r'(?<![\[{:,\s])"(?![}\],])', '"', dict_str)
                return json.loads(dict_str)
            
        except Exception as e:
            print(f"JSON解析错误: {str(e)}")
            print(f"原始内容: {output_text}")
        
        print("无法解析选择器结果")
        return {}
            
        
class SQLDesignerAgent(AgentNode):
    def __init__(self):
        super().__init__("sql_designer")
        self.chat_model = ChatOpenAI(
            openai_api_base=os.getenv("OPENAI_API_BASE"),
            model='gpt-4o',
            temperature=0.6
        )
    
    def process(self, context: AgentContext) -> Dict[str, Any]:
        try:
            selected_tables = context.intermediate_results.get("selected_tables", {})
            
            db_manager = DatabaseManager(context.db_name)
            keys_info = db_manager.get_primary_foreign_keys()
            
            # 获取表和列描述信息，不包含示例值
            description = self._get_table_column_descriptions(
                selected_tables, 
                context.db_name
            )
                        
            sql_candidates = []
            
            # 生成3个候选SQL，全部使用o1-like思维链
            for run in range(3):
                print(f"使用o1-like思维链生成SQL... (运行 {run + 1}/3)")
                o1_query = self._construct_o1_sql_query(
                    question=context.question,
                    hint=context.hint,
                    description=description,
                    filter_table_column=selected_tables,
                    primary_keys=keys_info["primary_keys"],
                    foreign_keys=keys_info["foreign_keys"]
                )
                o1_result = generate_o1_reasoning(o1_query)
                sql_candidates.append(o1_result)
            
            # 清理SQL结果
            cleaned_sql_candidates = self._clean_sql_results(sql_candidates)
            
            # 打印SQL查询来源
            print("\n===== SQL查询来源 =====")
            for i, sql in enumerate(cleaned_sql_candidates):
                print(f"\nSQL {i+1}: {sql[:100]}..." if len(sql) > 100 else f"SQL {i+1}: {sql}")
            
            return {
                "sql_candidates": cleaned_sql_candidates
            }
        except Exception as e:
            print(f"SQLDesignerAgent错误: {str(e)}")
            raise
    
    def _get_table_column_descriptions(self, all_tables_columns: Dict[str, List[str]], db_name: str) -> Dict[str, Dict]:
        import pandas as pd
        table_column_description = {}
        base_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", db_name)
        
        encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']

        for table_name, column_names in all_tables_columns.items():
            table_path = os.path.join(base_path, "database_description", f"{table_name}.csv")
            table_description = None
            
            for encoding in encodings:
                try:
                    table_description = pd.read_csv(table_path, encoding=encoding)
                    break  
                except UnicodeDecodeError:
                    continue  
                except Exception as e:
                    print(f"读取表{table_name}时发生非编码错误: {str(e)}")
                    break
                
            if table_description is None:
                print(f"表{table_name}的描述文件无法使用任何编码读取")
                table_column_description[table_name] = {}
                continue
            
            try:
                column_description = {}
                
                for column_name in column_names:
                    result = table_description[
                        table_description['original_column_name'].str.contains(
                            column_name, case=False, na=False, regex=False
                        )
                    ]
                    
                    if not result.empty:
                        column_info = {
                            "column_description": result.iloc[0].get('column_description', ''),
                            "value_description": result.iloc[0].get('value_description', '')
                        }
                        column_description[column_name] = column_info
                    else:
                        column_description[column_name] = {
                            "column_description": "",
                            "value_description": ""
                        }
                
                table_column_description[table_name] = column_description
            except Exception as e:
                print(f"处理表 {table_name} 描述时出错: {str(e)}")
                table_column_description[table_name] = {}
            
        return table_column_description
    
    def _construct_o1_sql_query(self, **kwargs) -> str:
       
        return f'''
    Question: {kwargs['question']}
    Hint: {kwargs['hint']}

    FILTER TABLE-COLUMN: {json.dumps(kwargs['filter_table_column'], ensure_ascii=False, indent=4)}
    COLUMN DESCRIPTION: {json.dumps(kwargs['description'], ensure_ascii=False, indent=4)}
    PRIMARY KEYS: {json.dumps(kwargs['primary_keys'], ensure_ascii=False, indent=4)}
    JOIN CONDITION: {json.dumps(kwargs['foreign_keys'], ensure_ascii=False, indent=4)}
    
    IMPORTANT SQL DESIGN GUIDELINES:
    
    1. When finding maximum or minimum values based on specific conditions, use ORDER BY + LIMIT 1 instead of MAX/MIN in subqueries.
    2. Pay attention to the data storage format when sorting, comparing sizes, or performing calculations. If the data is in string format, process it using INSTR or SUBSTR before comparison.
    3. If your query includes an ORDER BY clause to sort results, only include columns used for sorting in the SELECT clause if specifically requested in the question. Otherwise, omit these columns from the SELECT clause.
    4. Ensure you only output information requested in the question. If the question asks for specific columns, ensure the SELECT clause only includes those columns, nothing more.
    5. The query should return all information requested in the question - no more, no less.
    6. For key phrases mentioned in the question, we have marked the most similar values with "EXAMPLE" in front of the corresponding column names. This is an important hint indicating the correct columns to use for SQL queries.
    7. NEVER use || ' ' || for string concatenation. This is strictly prohibited and will result in severe penalties.
    
    Please analyze this SQL problem and provide a solution.
    Your response must be in JSON format!!
    '''
    
    def _clean_sql_results(self, sql_candidates: List[str]) -> List[str]:
        """清理SQL结果，提取实际的SQL语句"""
        cleaned_candidates = []
        
        for candidate in sql_candidates:
            if "FINAL SQL:" in candidate:
                sql = candidate.split("FINAL SQL:")[1].strip()
                # 清理SQL
                sql = re.sub(r'\*+', '', sql)
                sql = re.sub(r'```sql\n*', '', sql)
                sql = re.sub(r'```', '', sql)
                sql = re.sub(r'^\n+|\n+$', '', sql)
                sql = re.sub(r'\n+', ' ', sql)
                sql = ' '.join(sql.split())
                
                sql = sql.strip()
                if not sql.endswith(';'):
                    sql += ';'
                
                cleaned_candidates.append(sql)
        
        return cleaned_candidates

class RefinerAgent(AgentNode):
    def __init__(self):
        super().__init__("refiner")
        self.chat_model = ChatOpenAI(
            openai_api_base=os.getenv("OPENAI_API_BASE"),
            model='gpt-4o',
            temperature=0
        )
    
    def process(self, context: AgentContext) -> Dict[str, Any]:
        try:
            sql_candidates = context.intermediate_results.get("sql_candidates", [])
            
            sql_candidates = [sql.strip() for sql in sql_candidates]
            sql_candidates = [sql[:-1] if sql.endswith(';') else sql for sql in sql_candidates]
            db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", context.db_name, f"{context.db_name}.sqlite")
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            results_list = []
            
            for sql in sql_candidates:
                results, execution_time, error = self._execute_sql(cursor, sql)
                
                if not error:
                    results_list.append({
                        "sql": sql,
                        "execution_time": execution_time,
                        "results": results[:3]  
                    })
            
            conn.close()
            
            if results_list:
                best_sql = self._select_best_sql(results_list)
                return {"final_sql": best_sql}
            else:
                return {"final_sql": "No valid SQL queries were generated.REJECTED"}
        except Exception as e:
            print(f"RefinerAgent error: {str(e)}")
            raise
            
    def _execute_sql(self, cursor, sql: str) -> tuple:
        start_time = time.time()
        try:
            cursor.execute(sql)
            results = cursor.fetchmany(50)
            execution_time = time.time() - start_time
            return results, execution_time, None
        except sqlite3.Error as e:
            return None, None, str(e)
            
    def _select_best_sql(self, results_list: List[Dict]) -> str:
        result_groups = defaultdict(list)
        for entry in results_list:
            sorted_rows = sorted([tuple(row) for row in entry["results"]], key=lambda x: x)
            group_key = tuple(sorted_rows)
            print(group_key)
            result_groups[group_key].append( (entry["sql"], entry["execution_time"]) )
        
        if not result_groups:
            return "No valid SQL queries generated.REJECTED"
        
        
        max_votes = max(len(group) for group in result_groups.values())
        
        candidate_groups = [
            group for group in result_groups.values()
            if len(group) == max_votes
        ]
        
        min_exec_queries = []
        for group in candidate_groups:
            fastest_in_group = min(group, key=lambda x: x[1])
            min_exec_queries.append(fastest_in_group)


        best_query = min(min_exec_queries, key=lambda x: x[1])
        return best_query[0]

class AgentExecutor:
    def __init__(self):
        self._nodes: List[Node] = []
    
    def add_node(self, node: Node) -> None:
        self._nodes.append(node)
    
    def execute(self, question: str, hint: str, db_name: str, db_schema: Optional[Dict[str, List[str]]] = None) -> Dict[str, Any]:
        context = AgentContext(
            question=question,
            hint=hint,
            db_name=db_name,
            intermediate_results={}
        )
        
        # 如果提供了数据库结构，添加到中间结果中
        if db_schema is not None:
            context.intermediate_results["db_schema"] = db_schema
        
        try:
            for node in self._nodes:
                print(f"\n{'='*50}")
                if isinstance(node, AgentNode):
                    print(f"Agent: {node.name}")
                else:
                    print(f"Tool: {node.name}")
                result = node.process(context)
                print(f"输出结果:")
                print(result)
                context.intermediate_results.update(result)
            return context.intermediate_results
        except Exception as e:
            raise

def main():
    """主函数"""
    dotenv.load_dotenv()
    
    # 创建执行器
    executor = AgentExecutor()
    
    # 添加核心Agent
    executor.add_node(TableAgent())
    executor.add_node(SQLDesignerAgent())
    executor.add_node(RefinerAgent())
    
    # 测试问题
    question = "有多少男患者的总胆红素水平升高？"
    hint = "男性指SEX = 'M'；升高意味着高于正常范围；总胆红素高于正常范围是指`T-BIL` >= '2.0'"
    db_name = "thrombosis_prediction"
    
    try:
        # 执行流程
        results = executor.execute(question, hint, db_name)
        
        # 打印最终SQL
        print(f"\n最终SQL: {results.get('final_sql', '未生成SQL')}")
    except Exception as e:
        print(f"错误: {str(e)}")

if __name__ == "__main__":
    main()