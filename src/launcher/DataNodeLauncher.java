package launcher;

import api.DataNode;
import api.DataNodeHelper;
import impl.DataNodeImpl;
import org.omg.CORBA.ORB;
import org.omg.CosNaming.*;
import org.omg.CosNaming.NamingContextPackage.CannotProceed;
import org.omg.CosNaming.NamingContextPackage.InvalidName;
import org.omg.CosNaming.NamingContextPackage.NotFound;
import org.omg.PortableServer.POA;
import org.omg.PortableServer.POAHelper;
import org.omg.PortableServer.POAManagerPackage.AdapterInactive;
import org.omg.PortableServer.POAPackage.ServantNotActive;
import org.omg.PortableServer.POAPackage.WrongPolicy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class DataNodeLauncher {


    public static void main(String[] args){

        try {
            Properties properties = new Properties();
            properties.put("org.omg.CORBA.ORBInitialHost", "127.0.0.1");
            properties.put("org.omg.CORBA.ORBInitialHost", "1050");

            ORB orb = ORB.init(args, properties);


            POA rootpoa = POAHelper.narrow(orb.resolve_initial_references("RootPOA"));
            rootpoa.the_POAManager().activate();

            DataNodeImpl dataNodeServant = new DataNodeImpl();
            //export
            org.omg.CORBA.Object ref = rootpoa.servant_to_reference(dataNodeServant);
            DataNode href = DataNodeHelper.narrow(ref);

            //Naming context
            org.omg.CORBA.Object objRef = orb.resolve_initial_references("NameService");
            NamingContextExt ncRef = NamingContextExtHelper.narrow(objRef);

            // 创建一个映射来存储命令行参数
            HashMap<String, String> argMap = new HashMap<>();

            // 解析命令行参数
            for (int i = 0; i < args.length-1; i++) {
                if (args[i].startsWith("-")) {
                    // 如果参数以 "-" 开头，将其作为键，下一个参数作为值存入映射
                    argMap.put(args[i], args[i + 1]);
                    i++;  // 跳过下一个参数，因为已经处理过了
                }
            }

            // 获取DatanodeName的值
            String datanodeName = argMap.get("-DatanodeName");

            // 将新名称绑定到对象
            NameComponent[] path = ncRef.to_name(datanodeName);
            ncRef.rebind(path, href);

            //bind to naming
            //NameComponent[] path = ncRef.to_name("DataNode");
            //ncRef.rebind(path, href);
            System.out.printf(datanodeName+" is ready and waiting...");

            //waiting name
            orb.run();
        } catch (InvalidName e) {
            throw new RuntimeException(e);
        } catch (AdapterInactive e) {
            throw new RuntimeException(e);
        } catch (CannotProceed e) {
            throw new RuntimeException(e);
        } catch (NotFound e) {
            throw new RuntimeException(e);
        } catch (org.omg.CORBA.ORBPackage.InvalidName e) {
            throw new RuntimeException(e);
        } catch (WrongPolicy e) {
            throw new RuntimeException(e);
        } catch (ServantNotActive e) {
            throw new RuntimeException(e);
        } finally {

        }
    }
}
