public static void main(String[] args){
    try {
        Properties properties = new Properties();
        properties.put("org.omg.CORBA.ORBInitialHost","127.0.0.1");
        properties.put("org.omg.CORBA.ORBInitialHost","1050");

        ORB orb= ORB.init(args properties);

        POA rootpoa = POAHeler.narrow(rb.resolve_initial_reference("RootPOA"))
        rotpoa.the_POAManager().activate();

        DataNodeImpl dataNodeServant = new DataNodeImpl();

        org.omg.CORBA.Object ref = rootpoa.dataNodeServant_to_reference(dataNodeServant);
        DataNode href = DataNodeHelper.narrow(ref);

        //Naming context
        org.omg.CORBA.Object objRef = orb.resolve_initial_reference("NameService");
        NamingContextExt ncRef = NamingContextExtHelper.narrow(objRef);
        //bind to naming
        NameComponent[] path = ncRef.to_name("DataNode");
        ncRef.rebind(path,href);
        System.out.printf("DataNode is ready and waiting...");

        //waiting
        orb.run();
    }
}