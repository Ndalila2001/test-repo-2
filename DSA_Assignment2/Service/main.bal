import ballerinax/kafka;
import ballerina/sql;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;
import ballerina/http;
import ballerina/io;
import ballerina/uuid;



listener kafka:Listener logisticConsumer = check new(kafka:DEFAULT_URL, {
    groupId: "logistic-delivery-group",
    topics: "confirmationShipment"
});
service /logistic on new http:Listener(9090) {
    private final mysql:Client db;
    private final kafka:Producer kafkaProducer;

    function init() returns error? {
        self.kafkaProducer = check new(kafka:DEFAULT_URL);
        
        self.db = check new("localhost", "root", "@iCui4Cu123456", "logistics_db",3306);
    }

    resource function post sendPackage(ShipmentDetails req) returns error? {    
            
            sql:ParameterizedQuery customer_query = `SELECT id FROM Customers WHERE contact_number = ${req.contactNumber}`;
            
            //Customer|sql:Error customer_result = check self.db->queryRow(customer_query);
            //int customer_id = 0;
            //if customer_result is  Customer {
            //     customer_id = customer_result.id;
            //}
            //else {  
                sql:ParameterizedQuery insertCustomerQuery = `INSERT INTO Customers (first_name, last_name, contact_number) VALUES (${req.firstName}, ${req.lastName}, ${req.contactNumber})`;
                sql:ExecutionResult _ = check self.db->execute(insertCustomerQuery);
            //}

            req.trackingNumber = uuid:createType1AsString();
            sql:ParameterizedQuery insertShipmentQuery =    `INSERT INTO Shipments ( shipment_type, pickup_location, delivery_location, preferred_time_slot, tracking_number) 
                                                            VALUES ( ${req.shipmentType}, ${req.pickupLocation}, ${req.deliveryLocation}, ${req.preferredTimeSlot}, ${req.trackingNumber})`;
            
            sql:ExecutionResult _ = check self.db->execute(insertShipmentQuery);

            check self.kafkaProducer->send({topic: req.shipmentType, value: req});
            
            //string shipment_type = req.shipmentType;
            //string pickup_location = req.pickupLocation;
            //string delivery_location = req.deliveryLocation;
            //string preffered_time = req.preferredTimeSlot;
            //string first_name = req.firstName;
            //string last_name = req.lastName;
            //string contact_number = req.contactNumber;
            //sql:ExecutionResult _ = check self.db->execute(`INSERT INTO Customers (first_name, last_name, contact_number) VALUES (${first_name}, ${last_name}, ${contact_number})`);
            //sql:ExecutionResult _ = check self.db->execute(`INSERT INTO Shipments (shipment_type, pickup_location, delivery_location, preferred_time_slot) VALUES (${shipment_type}, ${pickup_location}, ${delivery_location}, ${preffered_time})`); 

           //check self.kafkaProducer->send({topic: shipment_type, value: req});

           
    }

    
    
    
}

service on logisticConsumer{
    private final mysql:Client db;
    

    function init() returns error?{
        self.db = check new("localhost", "root", "@iCui4Cu123456", "logistics_db",3306);
    }
    remote function onConsumerRecord(ShipmentConfirmation[] req) returns error? {
        io:println(req);
        foreach ShipmentConfirmation confirmation in req {
            sql:ParameterizedQuery updateQuery = `UPDATE Shipments 
                SET status = "confirmed", estimated_delivery_time = ${confirmation.estimatedDeliveryTime} 
                WHERE tracking_number = ${confirmation.confirmationId}`;

            sql:ExecutionResult _ = check self.db->execute(updateQuery);
        }

    }
}

type ShipmentDetails record {
    string shipmentType;
    string pickupLocation;
    string deliveryLocation;
    string preferredTimeSlot;
    string firstName;
    string lastName;
    string contactNumber;
    string trackingNumber = "";
};

type ShipmentConfirmation record {
    string confirmationId;
    string shipmentType;
    string pickupLocation;
    string deliveryLocation;
    string estimatedDeliveryTime;
    string status;
};

type Customer record {
    int id;
    string firstName;
    string lastName;
    string contactNumber;
};
