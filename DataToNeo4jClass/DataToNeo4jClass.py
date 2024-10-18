from py2neo import Graph, Node, Relationship, NodeMatcher

class DataToNeo4j:
    
    def __init__(self):
        #创建neo4j连接
        link = Graph ("http://localhost:7474",  auth=("neo4j", "123456"))
        self.graph = link
        #清空
        self.graph.delete_all()
        self.matcher = NodeMatcher(link)
        
        """建立lable"""
        self.Tester_Resource = "Tester_Resource"
        self.Test_Items = "Test_Items"
        
    def create_node(self, node_Test_Items_key,node_Tester_Resource_key):
        """建立节点"""
        for name in node_Tester_Resource_key:
            Tester_Resource_node = Node(self.Tester_Resource, name=name)
            self.graph.create(Tester_Resource_node)
        for name in node_Test_Items_key:
            Test_Items_node = Node(self.Test_Items, name=name)
            self.graph.create(Test_Items_node)
            
        
    def create_relation(self, df_data):
        """建立联系"""      
        m = 0
        for m in range(0, len(df_data)):
            try:    
                # print(list(self.matcher.match(self.Tester_Resource).where("_.name=" + "'" + df_data['Tester_Resource'][m] + "'")))
                # print(list(self.matcher.match(self.Test_Items).where("_.name=" + "'" + df_data['Test_Items'][m] + "'")))
                rel = Relationship(self.matcher.match(self.Test_Items).where("_.name=" + "'" + df_data['Test_Items'][m] + "'").first(),
                                df_data['method'][m], self.matcher.match(self.Tester_Resource).where("_.name=" + "'" + df_data['Tester_Resource'][m] + "'").first())

                self.graph.create(rel)
            except AttributeError as e:
                print(e, m)
            