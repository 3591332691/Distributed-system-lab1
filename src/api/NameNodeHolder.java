package api;


public final class NameNodeHolder implements org.omg.CORBA.portable.Streamable
{
  public api.NameNode value = null;

  public NameNodeHolder ()
  {
  }

  public NameNodeHolder (api.NameNode initialValue)
  {
    value = initialValue;
  }

  public void _read (org.omg.CORBA.portable.InputStream i)
  {
    value = api.NameNodeHelper.read (i);
  }

  public void _write (org.omg.CORBA.portable.OutputStream o)
  {
    api.NameNodeHelper.write (o, value);
  }

  public org.omg.CORBA.TypeCode _type ()
  {
    return api.NameNodeHelper.type ();
  }

}
