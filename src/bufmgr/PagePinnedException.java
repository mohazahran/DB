package bufmgr;
import chainexception.*;

public class PagePinnedException extends ChainException {
  
  public PagePinnedException(Exception e, String name)
    {
      super(e, name); 
    }

    PagePinnedException() {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}

