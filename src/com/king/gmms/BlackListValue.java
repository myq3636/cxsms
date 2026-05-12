package com.king.gmms;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2006</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public class BlackListValue {
    private String o_number;
    private String r_number;
    private int o_op=-1;
    private int r_op=-1;

    public BlackListValue(String o_number,String r_number,int o_op,int r_op) {
        this.o_number=o_number;
        this.r_number=r_number;
        if (o_op>0)
            this.o_op=o_op;
        else
            this.o_op=0;
        if (r_op>0)
            this.r_op=r_op;
        else
            this.r_op=0;
    }

    public String getONumber(){
        return o_number;
    }

    public String getRNumber(){
       return r_number;
   }

   public int getOOP(){
       return o_op;
   }

   public int getROP(){
       return r_op;
   }

}
