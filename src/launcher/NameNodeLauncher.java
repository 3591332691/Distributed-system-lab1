public static void main(String[] args){
    try {
        Properties properties = new Properties();
        properties.put("org.omg.CORBA.ORBInitialHost","127.0.0.1");
        properties.put("org.omg.CORBA.ORBInitialHost","1050");

        ORB orb= ORB.init(args properties);

        POA rootpoa = POAHeler.narrow(rb.resolve_initial_reference("RootPOA"))
        rotpoa.the_POAManager().activate();

        NameNodeImpl nameNodeServant = new NameNodeImpl();

        org.omg.CORBA.Object ref = rootpoa.nameNodeServant_to_reference(nameNodeServant);
        NameNode href = NameNodeHelper.narrow(ref);

        //Naming context
        org.omg.CORBA.Object objRef = orb.resolve_initial_reference("NameService");
        NamingContextExt ncRef = NamingContextExtHelper.narrow(objRef);
        //bind to naming
        NameComponent[] path = ncRef.to_name("NameNode");
        ncRef.rebind(path,href);
        System.out.printf("NameNode is ready and waiting...");

        //waiting
        orb.run();
    }
}