package yanbinwa.common.zNodedata.decorate;

import org.junit.Test;

public class ZNodeDecorateTypeTest
{

    @Test
    public void test()
    {
        ZNodeDecorateType type = ZNodeDecorateType.KAFKA;
        System.out.println(type.name());
        
        @SuppressWarnings("unused")
        ZNodeDecorateType type1 = ZNodeDecorateType.valueOf("KAFKA");
        @SuppressWarnings("unused")
        ZNodeDecorateType type2 = ZNodeDecorateType.valueOf("REDIS");
        ZNodeDecorateType type3 = null;
        try
        {
            type3 = ZNodeDecorateType.valueOf("XXXXX");
        }
        catch(IllegalArgumentException e)
        {
            type3 = null;
        }
        
        if (type3 == null)
        {
            System.out.println("type3 is null");
        }
        else
        {
            System.out.println("type3 is not null");
        }
    }

}
