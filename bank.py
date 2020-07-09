import apache_beam as beam
import datetime as dt

p1 = beam.Pipeline()

# ParDo Function
class Counting(beam.DoFn):
  
  def process(self, element):
    key, values = element
    return [(str(key) +' , '+ str(values) + ' - Defaulter')]

# Composite Transform
class MyTransform(beam.PTransform):
  
  def expand(self, input_coll):
    
    a = ( 
        input_coll
                       | 'count filter accounts' >> beam.Filter(filter_on_count)
        )
    return a
  
def filter_on_count(elements):
    id,value = elements
    return  value > 0
  
 # PTransform 
def createKeyVal(elem):
    cust_id,fName,lName,rel_id,types,cashLimit,spent,withdrawn,amt_paid,date = elem.split(',')
    key =   cust_id+','+ fName +' '+ lName
    spent = int(spent)
    cashLimit = int(cashLimit)
    withdrawn = int(withdrawn)
    amt_paid = int(amt_paid)
    
    def_points = 0
    
    if(amt_paid < (spent * 0.7)):
        def_points +=1
    
    if((amt_paid  == cashLimit) and (amt_paid < spent)):
        def_points +=1
    
    if((amt_paid  == cashLimit) and (amt_paid < (spent * 0.7))):
        def_points +=1
            
    return (key,def_points)

cards = (
        p1
        | beam.io.ReadFromText('/home/dwarika/Downloads/bank/cards.txt',skip_header_lines = 1)
        | beam.Map(createKeyVal)
        | beam.Filter(filter_on_count)
        | MyTransform()
        | beam.CombinePerKey(sum)
        | beam.ParDo(Counting())
        #| beam.Map(print)
        )

medical = (
        p1
        | "Load Medical Data" >> beam.io.ReadFromText('/home/dwarika/Downloads/bank/loan.txt',skip_header_lines = 1)
        | "Split String" >> beam.Map(lambda elem : elem.split(","))
        | "Filter Medical" >> beam.Filter(lambda elem : elem[5] == 'Medical Loan' and elem[8] > elem[6])
        | "Extract Key/Value" >> beam.Map(lambda elem: (elem[0]+', '+elem[1], int(dt.datetime.strptime(elem[6], '%d-%m-%Y').month)))
        | "Payemnts more than 3" >> beam.Filter(lambda elem: elem[1] > 3)
        | "group map reduce" >> beam.CombinePerKey(sum)
       # | "Display Medical" >> beam.Map(print)
        )
combine_output = (
    (cards, medical)
    | "Combine All" >> beam.Flatten()
    | "Write O/P" >> beam.io.WriteToText('/home/dwarika/Learning/GCP/dataflow/out/final')
)
p1.run()