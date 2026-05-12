package com.king.gmms.domain;

import java.util.ArrayList;
import java.util.HashMap;

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
public class Group {
    ArrayList<AttrPair> attrs= new ArrayList<AttrPair>();
     // Not safe. Make sure the attribute name is unique before use this map.
     // In one group, there might be more than one attributes with same name.
     // In this situation, return value of getAttr(String) is not specified.

     HashMap<String, AttrPair> attrMap =new HashMap<String, AttrPair>();

     private AttrPair preAttrPair = null;
     private boolean append = false;

     public Group()
     {}
     public void addAttr(AttrPair attr)
     {
         if (attr==null || attr.getItem()==null) return;

         //If append flag is open and attr.item="", should append its value after preAttrPair
         if(append && "".equalsIgnoreCase(attr.getItem())){
             this.appendAttr(attr);
             return;
         }

         //else, add the attr into attrs and attrMap normally
         attrs.add(attr);
         attrMap.put(attr.getItem().toLowerCase(),attr);
         this.preAttrPair = attr;

         if(!"".equalsIgnoreCase(preAttrPair.getItem()))
             append = true;
     }
     public void addAttr(String item, String value)
     {
         if (item==null) return;
         AttrPair attr=new AttrPair(item,value);
         attrs.add(attr);
         attrMap.put(item.trim().toLowerCase(),attr);
         this.preAttrPair = attr;
     }

     private void appendAttr(AttrPair attr){
         //Append the attr's value after preAttrPair
         this.appendValue(attr);

         attrMap.put(preAttrPair.getItem().toLowerCase(),preAttrPair);

         //For attrs, replace the last attrPair is ok.
         attrs.remove(attrs.size()-1);
         attrs.add(preAttrPair);
     }


     public AttrPair getAttr(String name)
     {
         if (name==null) return null;
         return attrMap.get(name.trim().toLowerCase());
     }

     public ArrayList<AttrPair> getAllAttrs()
     {
         return attrs;
     }

     /**
      * Process the "(A=a,B=b1,b2,C=c)" case
      * We should append b2 after b1 and treat "b1,b2" as B's value.
      * In this case, the attr parameter should be following format:
      *   1. item="";
      *   2. value="b2";
      *
      * Note: The current logic can't handle the following cases
      *   1.  value including both "," and "="
      *   2.  (b1, A=a, B=b, ...) case
      *
      * Needn't care attr==null or attr.item==null case here
      */
     private void appendValue(AttrPair attr){
         String old_value = this.preAttrPair.getStringValue();

         this.preAttrPair.setValue(old_value + "," + attr.getStringValue());
     }

}
