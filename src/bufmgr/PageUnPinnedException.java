package bufmgr;
import chainexception.*;

public class PageUnPinnedException extends ChainException {
  
  public PageUnPinnedException(Exception e, String name)
    {
      super(e, name); 
    }
}

