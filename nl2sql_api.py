import json
import os
import dotenv
from typing import Dict, Any, List, Optional
from simplified_nl2sql import TableAgent, SQLDesignerAgent, RefinerAgent, AgentContext, AgentExecutor

class NL2SQLGenerator:
    
    def __init__(self, load_env: bool = True):
        if load_env:
            dotenv.load_dotenv()
        
        # 创建执行器和添加核心Agent
        self.executor = AgentExecutor()
        self.executor.add_node(TableAgent())
        self.executor.add_node(SQLDesignerAgent())
        self.executor.add_node(RefinerAgent())
    
    def generate_sql(self, 
                    question: str, 
                    hint: str, 
                    db_name: str,
                    db_schema: Optional[Dict[str, List[str]]] = None,
                    verbose: bool = False) -> Dict[str, Any]:
        try:
            self._set_verbose(verbose)
            
            results = self.executor.execute(question, hint, db_name)
            
            final_sql = results.get("final_sql", "")
            
            if "REJECTED" in final_sql:
                return {
                    "sql": "",
                    "status": "error",
                    "message": "无法生成有效的SQL查询"
                }
            
            return {
                "sql": final_sql,
                "status": "success",
                "message": "成功生成SQL查询",
                "details": results if verbose else {}
            }
            
        except Exception as e:
            return {
                "sql": "",
                "status": "error",
                "message": f"生成SQL时发生错误: {str(e)}"
            }
    
    def _set_verbose(self, verbose: bool):
        import builtins
        
        original_print = builtins.print
        
        if not verbose:
            builtins.print = lambda *args, **kwargs: None

        def restore_print():
            builtins.print = original_print
            
        return restore_print


def example_usage():
    generator = NL2SQLGenerator()
    
    question = "有多少男患者的总胆红素水平升高？"
    hint = "男性指SEX = 'M'；升高意味着高于正常范围；总胆红素高于正常范围是指`T-BIL` >= '2.0'"
    db_name = "thrombosis_prediction"
    
    result = generator.generate_sql(question, hint, db_name, verbose=True)
    
    print("\n===== 生成结果 =====")
    print(f"生成情况: {result['status']}")
    print(f"消息: {result['message']}")
    print(f"SQL: {result['sql']}")


if __name__ == "__main__":
    example_usage()