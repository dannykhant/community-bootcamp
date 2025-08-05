# Python for Data Engineering

- Interpreter
    - Step-1
        - code written → bytecode (.pyc)
        - bytecode is low-level code that Python understands
    - Step-2
        - PVM executed bytecode line by line during runtime
- print()
    - print(”hello”, sep=”|”)
    - double “” | ‘’ to escape “ | ‘ - same like \
- Variable
    - x, y, z = 1, 2, 3
    - x = y = z = 3
    - type(x) to see data type
    - int(var) to convert to integer (explicit type casting)
- \ or () to escape for multiple lines
- String Manipulation
    - x = “abcde fg” → x[1] → #b
    - Slicing
        - x[0:4] → #abc
        - x[:] → #abcde ← x[0:len(x)]
    - Spliting
        - x.split(”|”) → #[abcde, fg]
    - filename.endswith(”csv”) → #bool
    - x.count(”a”) → #1
    - x.isnumeric() → #False
    - x.isalnum() → #False (alphanumeric)
- Iterations
    - range(1, 101) → 1 to 100
- Data Structures
    - x = [1, 2, 3, 4, 5]
    - List Slicing
        - To get the last 3 items
            - x[-3:] → [3, 4, 5] ← x[len()-3:len()]
        - Step: x[::2] → [1, 3, 5]
    - List Methods
        - x.insert(1, 9) → [1, 9, 2, 3, 4, 5]
        - x.append(9) → [1, 2, 3, 4, 5, 9]
        - x.pop() → [1, 2, 3, 4]
        - x.reverse() = x[::-1] = reversed(x)
    - List Comprehension
        - [i for i in x if i%2 == 0] → [2, 4]
    - Dictionary Methods
        - mydict.keys()
        - mydict.values()
        - mydict.items()
    - Sets
        - set()
        - No dups and allows to do Math
        - a = {1, 2, 3} | b = {3, 4}
        - Set Methods
            - a.union(b) → {1, 2, 3, 4}
            - a.intersection(b) → {3}
            - a.remove(2) → {1, 3}
            - a.add(9) → {1, 2, 3, 9}
    - Tuple
        - (1, 3, 4)
        - Immutable
- Functions
    - def func(a, *b) → *b is tuple
    - def func(a, **b) → **b is dict
    - lambda x, y: x + y
    - Map
        - map(lambda i: i * i, a) → [1, 4, 9]
    - Filter
        - filter(lambda i: i % 2 == 0, a) → [2]
    - Reduce
        - reduce(lambda i, j: i * j, a) → 6
- Exception Handling
    - try → except → finally (always run even after function return)
- Enumerate
    - enumerate() → (index, value)
- OOP
    - Class
        
        ```python
        class Predator():
        	gene = "Carnivore"
        	
        	# Constructor
        	def __init__(self, color):
        		self.color = color
        	
        	def show_info(self):
        		print(self.gene, self.color)
        ```
        
    - Object Instantiation
        
        ```python
        cat = Predator("black")
        cat.show_gene() # Predator.show_gene(cat)
        ```
        
    - Methods
        - Instance Method
            
            ```python
            class Predator():
            	def show_info(self):
            		print("this is info")
            ```
            
        - Static Method
            
            ```python
            class Predator():
            	@staticmethod
            	def calc(x, y):
            		print(x + y)
            ```
            
        - Class Method
            
            ```python
            class Student():
            	name = "Danny"
            	@classmethod
            	def change_name(cls, name):
            		cls.name = name
            ```
            
        - Magic/Dunder Method
            
            ```python
            class Dog:
            	def __str__(self):
            		return "This is dog"
            		
            my_dog = Dog()
            print(my_dog) # This is dog
            ```
            
- Getters & Setters
    
    ```python
    class Animal():
    	name = "Angel"
    	
    	@property
    	def info(self): # Getter
    		return self.name
    		
    	@info.setter
    	def info(self, new_name): # Setter
    		self.name = new_name
    		
    cat = Animal()
    cat.info = "Holy" # Setter
    
    print(cat.info) # Getter
    ```
    
- Inheritance
    
    ```python
    class Animal():
    	num_of_legs = 4
    	def __init__(self, name):
    		self.animal_name = name
    	def count_legs(self):
    		print(self.animal_name, self.num_of_legs)
    	
    class Predator(Animal):
    	gene = "Carnivore"
    	def __init__(self, name, animal_name):
    		self.predator_name = name
    		Animal.__init__(self, animal_name) # self.animal_name = animal_name
    	def show_info(self):
    		print(self.predator_name, self.gene)
    	def this_count_legs(self):
    		Animal.count_legs(self) # super().count_legs()
    		
    cat = Predator("cat", "animal")
    cat.show_info()
    cat.this_count_legs()
    ```
    
    - Single Level
        
        ```python
        class Animal():
        	def __init__(self, has_legs):
        		self.has_legs = has_legs
        		
        class Predator(Animal):
        	def __init__(self, has_legs):
        		Animal.__init__(self, has_legs)
        	def show_info(self):
        		print(self.has_legs)
        		
        cat = Predator(True)
        cat.show_info()
        ```
        
    - Multi Level
        
        ```python
        class Company():
        	def __init__(self, company_name):
        		self.company_name = company_name
        
        class Department(Company):
        	def __init__(self, dept_name, company_name):
        		self.dept_name = dept_name
        		Company.__init__(self, company_name)
        
        class Employee(Department):
        	def __init__(self, employee_name, dept_name, company_name):
        		self.employee_name = employee_name
        		Department.__init__(self, dept_name, company_name)
        	def show_info(self):
        		print(self.employee_name, self.dept_name, self.company_name)
        
        emp1 = Employee("Rahul", "IT", "XYZ")
        emp1.show_info()
        ```
        
    - Multiple
        
        ```python
        class GroundAnimal():
        	def __init__(self, has_legs):
        		self.has_legs = has_legs
        class FlyingAnimal():
        	def __init__(self, has_wings):
        		self.has_wings = has_wings
        		
        class Predator(GroundAnimal, FlyingAnimal):
        	def __init__(self, has_legs, has_wings):
        		GroundAnimal.__init__(self, has_legs)
        		FlyingAnimal.__init__(self, has_wings)
        	def show_info(self):
        		print(self.has_legs, self.has_wings)
        		
        eagle = Predator(True, True)
        eagle.show_info()
        ```
        
- Multi Threading
    
    ```python
    import time
    import random
    from concurrent.futures import ThreadPoolExecutor
    
    tables = ["sales", "customers", "products"]
    
    def processing(table):
        elapsed = random.randint(1,5)
        start = time.ctime()
        time.sleep(elapsed)
        print(f"{table}: took {elapsed}s and started at {start}")
        
    with ThreadPoolExecutor(max_workers=len(tables)) as exec:
        futures = exec.map(processing, tables)
    ```
    
- Module - requests
    
    ```python
    import requests
    
    response = requests.get("https://jsonplaceholder.typicode.com/todos/1")
    
    data = response.json()
    
    print(data)
    ```
    
- Module - os
    
    ```python
    import os
    
    os.makedirs("this_dir")
    
    # get the absolute path of the file running
    os.path.abspath(__file__)
    ```
    
- Module - dbutils, notebookutils