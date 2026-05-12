package com.king.gmms.util.charset;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;

public class Convert {
	
	private static final int[] upperBits = {
//      0xFF, // 0 = B7|B6|B5|B4|B3|B2|B1|B0
      0xFE, // 1 = B7|B6|B5|B4|B3|B2|B1
      0xFC, // 2 = B7|B6|B5|B4|B3|B2
      0xF8, // 3 = B7|B6|B5|B4|B3
      0xF0, // 4 = B7|B6|B5|B4
      0xE0, // 5 = B7|B6|B5
      0xC0, // 6 = B7|B6
      0x80  // 7 = B7 
  };
	 
	 private static final int[] lowerBits = {
	      0x01, // 0 =                   B0
	      0x03, // 1 =                B1|B0
	      0x07, // 2 =             B2|B1|B0
	      0x0F, // 3 =          B3|B2|B1|B0
	      0x1F, // 4 =       B4|B3|B2|B1|B0
	      0x3F, // 5 =    B5|B4|B3|B2|B1|B0
	      0x7F  // 6 = B6|B5|B4|B3|B2|B1|B0
	  };
	 
	 
	 private static final int[] upperBit = {
		      0x00, // 0 =  
		      0x40, // 1 = B6
		      0x60, // 2 = B6|B5
		      0x70, // 3 = B6|B5|B4
		      0x78, // 4 = B6|B5|B4|B3
		      0x7C, // 5 = B6|B5|B4|B3|B2
		      0x7E, // 6 = B6|B5|B4|B3|B2|B1
		      0x7F  // 7 = B6|B5|B4|B3|B2|B1|B0
		    }; 
		    
	private static final int[] lowerBit = {
		        0x7F, // 0 = B6|B5|B4|B3|B2|B1|B0
		        0x3F, // 1 =    B5|B4|B3|B2|B1|B0
		        0x1F, // 2 =       B4|B3|B2|B1|B0
		        0x0F, // 3 =          B3|B2|B1|B0
		        0x07, // 4 =             B2|B1|B0
		        0x03, // 5 =                B1|B0
		        0x01, // 6 =                   B0
		        0x00  // 7 =
		    };
	  public static final Integer[] gsm2ISO = new Integer[]{
		  -1,163,-1,165,232,233,249,236,242,199,
		  -1,216,248,-1,197,229,-1,-1,-1,-1,
		  -1,-1,-1,-1,-1,-1,-1,-1,198,230,
		  223,201,-1,-1,-1,-1,164,-1,-1,-1,
		  -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
		  -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
		  -1,-1,-1,-1,161,-1,-1,-1,-1,-1,
		  -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
		  -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
		  -1,196,214,209,220,167,191,-1,-1,-1,
		  -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
		  -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
		  -1,-1,-1,228,246,241,252,224,-1
	  };
	  public static final Integer[] gsm2UCS2 = new Integer[]{
		  -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
		  -1,-1,-1,-1,-1,-1,916,-1,934,915,
		  923,937,928,936,931,920,926,-1,-1,-1		  
	  };
	  public static final Integer[] gsm2ASCII = new Integer[]{
		  64,-1,36,-1,-1,-1,-1,-1,-1,-1,
		  10,-1,-1,13,-1,-1,-1,95,-1,-1,
		  -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
		  -1,-1,32,33,34,35,-1,37,38,39,
		  40,41,42,43,44,45,46,47,48,49,
		  50,51,52,53,54,55,56,57,58,59,
		  60,61,62,63,-1,65,66,67,68,69,
		  70,71,72,73,74,75,76,77,78,79,
		  80,81,82,83,84,85,86,87,88,89,
		  90,-1,-1,-1,-1,-1,-1,97,98,99,
		  100,101,102,103,104,105,106,107,108,109,
		  110,111,112,113,114,115,116,117,118,119,
		  120,121,122,-1,-1,-1,-1,-1,-1  
	  };
	  public static final Integer[] charest2GSM = new Integer[]{
		     -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1, //   0 -   9
		     10,  -1,  3466,  13,  -1,  -1,  -1,  -1,  -1,  -1, //  10 -  19
		     -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1, //  20 -  29
		     -1,  -1,  32,  33,  34,  35,   2,  37,  38,  39, //  30 -  39
		     40,  41,  42,  43,  44,  45,  46,  47,  48,  49, //  40 -  49
		     50,  51,  52,  53,  54,  55,  56,  57,  58,  59, //  50 -  59
		     60,  61,  62,  63,   0,  65,  66,  67,  68,  69, //  60 -  69
		     70,  71,  72,  73,  74,  75,  76,  77,  78,  79, //  70 -  79
		     80,  81,  82,  83,  84,  85,  86,  87,  88,  89, //  80 -  89
		     90,3516,3503,3518,3476,  17,  -1,  97,  98,  99, //  90 -  99
		    100, 101, 102, 103, 104, 105, 106, 107, 108, 109, // 100 - 109
		    110, 111, 112, 113, 114, 115, 116, 117, 118, 119, // 110 - 119
		    120, 121, 122,3496,3520,3497,3517,  -1,  -1,  -1, // 120 - 129
		     -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1, // 130 - 139
		     -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1, // 140 - 149
		     -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1, // 150 - 159
		     -1,  64,  -1,   1,  36,   3,  -1,  95,  -1,  -1, // 160 - 169
		     -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1, // 170 - 179
		     -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  -1, // 180 - 189
		     -1,  96,  -1,  -1,  -1,  -1,  91,  14,  28,   9, // 190 - 199
		     -1,  31,  -1,  -1,  -1,  -1,  -1,  -1,  -1,  93, // 200 - 209
		     -1,  -1,  -1,  -1,  92,  -1,  11,  -1,  -1,  -1, // 210 - 219
		     94,  -1,  -1,  30, 127,  -1,  -1,  -1, 123,  15, // 220 - 229
		     29,  -1,   4,   5,  -1,  -1,   7,  -1,  -1,  -1, // 230 - 239
		     -1, 125,   8,  -1,  -1,  -1, 124,  -1,  12,   6, // 240 - 249
		     -1,  -1, 126,  -1,  -1,  -1                      // 250 - 255
	  };	  	 	  
	  
	  
	  /**
	   * Converts an array of ints into an array of chars.
	   * 
	   * @param data The int array
	   * @return The char array
	   */
	  public static char[] intToChar(final int[] data) {
	    final int length = data.length;
	    final char[] output = new char[length];
	    for (int i = 0; i < length; i++) {
	      output[i] = (char)data[i];
	    }
	    return output;
	  }
	  
	  public static int[] charToint(final char[] data) {
		    final int length = data.length;
		    final int[] output = new int[length];
		    for (int i = 0; i < length; i++) {
		      output[i] = (int)data[i];
		    }
		    return output;
		  }
	  	  
	  public static byte[] convert2GSM(String data){
		  if (data == null) {
		      return null;
		    }
		    final int length       = data.length();
		    final int tblLength    = charest2GSM.length;		   
		    ArrayList<Integer> list = new ArrayList<Integer>();  
		    final char[] charArray = data.toCharArray();		    
		    
		    for (int i = 0; i < length; i++) {
		      int c = charArray[i];
		      int r = (c >= tblLength ? -1 : charest2GSM[c]);
		      if (r == -1) {
		        switch (c) {
		        case  916: r =   16; break; // Delta character
		        case  934: r =   18; break; // Phi character
		        case  915: r =   19; break; // Gamma character
		        case  923: r =   20; break; // Lambda character
		        case  937: r =   21; break; // Omega character
		        case  928: r =   22; break; // Pi character
		        case  936: r =   23; break; // Psi character
		        case  931: r =   24; break; // Sigma character
		        case  920: r =   25; break; // Tetha character
		        case  926: r =   26; break; // Xi character
		        case 8364: r = 3557; break; // Euro character

		        default:		          
		          r = 63;
		          break;
		        }
		      }
		      if (r >= 3456) { // to extension table
		          r = r - 3456;
		          list.add(27);
		        }
		        list.add(r);
	    }		    
		    byte gsm[] = new byte[list.size()];
		    for(int i =0; i<list.size();i++){
		    	int temp = list.get(i);
		    	gsm[i] = (byte)(temp & 0xff);
		    }
		    return gsm;
	  }
	  
	  public static String convertGSM2String(byte[] newContent) {
		  
		  int []chars = new int[newContent.length];
			int i = 0;
			int j = 0;
			for(i =0, j=0; i<newContent.length;i++,j++){
				//mapping ASCII characters
				int temp = newContent[i];
				int value = -1;
				if(temp<128&&temp>=0){
					value = Convert.gsm2ASCII[temp];
					if(value ==-1){                         //mapping ISO-8859-1 characters
						value = Convert.gsm2ISO[temp];
						if(value == -1){                    //mapping UCS2 characters
							if(temp<28){
								value = Convert.gsm2UCS2[temp];
								if(value ==-1){
									// mapping extended characters
									if(temp==27){
										int c = newContent[i+1];
										switch(c){
										case 10: value=12; break;
										case 20: value=94; break;
										case 40: value=123;  break;
										case 41: value=125; break;
										case 47: value=92; break;
										case 60: value=91; break;
										case 61: value=126; break;
										case 62: value=93;  break;
										case 64: value=124;  break;
										case 101: value=8364; break;
										default: value =-1;									 
										}
										if(value ==-1){
											 value = 63;
										 }else{										 
											 i++;
										 }
									}else{
										value = 63;
									}
								}
							}else{
								value = 63;
							}								
						}
					}
				}else{
					value = 63;
				}					
				chars[j]=value;
			}
			int tempContent[] = new int[j]; 
			System.arraycopy(chars, 0, tempContent, 0, j);
			return new String(Convert.intToChar(tempContent));
	  }
	  
	  public static byte[] encode7bit(byte t[], int fillBits) {
		    int ud[] = byte2int(t);			    
		    final int udl = ud.length;
		    final int rest = (udl * 7) % 8;
		    int ol = (udl * 7) / 8
		           + (rest + fillBits > 8 ? 1 : 0)
		           + (fillBits > 0 || rest > 0 ? 1 : 0);
		    
		    final int[] output = new int[ol--];
		    
		    int f = (rest + fillBits) % 8;
		    int c = (f == 0 ? 0 : 1);
		    int u;
		    for (u = udl - 1; u >= 0; u--) {
		      if (ol == 0) c = 0;
		      output[ol]     |= (ud[u] & upperBit[f]) >> 7 - f;
		      output[ol - c] |= (ud[u] & lowerBit[f]) << f + 1;
		      if (c == 0) c = 1;
		      else if (f != 7) ol--;
		      
		      if (++f == 8) f = 0;
		    }	      		      		    
		    byte gsm[] = new byte[output.length];
		    for(int i =0; i<output.length;i++){
		    	int temp = output[i];
		    	gsm[i] = (byte)(temp & 0xff);
		    }
		    return gsm;
		  }
	    		 	     
	       
	        
	     public static byte[] decodeGSM7Bit(byte[] buffer, int fillBits) {

	    	 final int length = buffer.length;	    	 
	    	 int ud[] = byte2int(buffer);
	    	 if (fillBits != 0) {
	    	      final int len = length - 1;
	    	      final int cut = lowerBits[fillBits - 1];
	    	      final int move = 8 - fillBits;
	    	      final int[] udCopy = new int[length];
	    	      for (int f = 0; f < len; f++) {
	    	        udCopy[f] = ud[f] >> fillBits;
	    	        udCopy[f] |= (ud[f + 1] & cut) << move;
	    	      }
	    	      udCopy[len] = ud[len] >> fillBits;
	    	      ud = udCopy;
	    	    }    
	    	    int udl = (length * 8 - fillBits) / 7;
	    	    final int[] output = new int[udl];
	    	    
	    	    int b = 6, p = 0;
	    	    for (int i = 0; i < udl; i++) {
	    	      switch (b) {
	    	      case 7: // U0
	    	        output[i] = (ud[p] & upperBits[0]) >> 1;
	    	    
	    	        break;
	    	        
	    	      case 6: // L6
	    	        output[i] = ud[p] & lowerBits[b];
	    	        break;
	    	        
	    	      default: // The rest
	    	        output[i] = ((ud[p] & lowerBits[b]) << (6 - b))
	    	                  + ((ud[p - 1] & upperBits[b + 1]) >> (b + 2));
	    	        break;
	    	      }
	    	      if (--b == -1) b = 7;
	    	      else p++;
	    	    }
	    	    byte gsm[] = new byte[output.length];
			    for(int i =0; i<output.length;i++){
			    	int temp = output[i];
			    	gsm[i] = (byte)(temp & 0xff);
			    }
	    	    return gsm;
	     }
	     public static int[] byte2int(byte a[]){
	    	 int temp[] = new int[a.length];
	    	 for(int i =0; i<a.length;i++){
	    		 temp[i] = a[i] & 0xFF;
	    	 }
	    	 return temp;
	     }
	     public static void main(String[] args) {
	    	 String test = "@$_^{}\\[~]|!\"#%&'()*+,-./0123456789:;<=>?ABCDEFGHIJKLMNOPQRS" +
	    	 		"TUVWXYZabcdefghijklmnopqrstuvwxyz£¥èéùìòÇØøÅåÆæßÉ¤¡ÄÖÑÜ§¿äöñüàΦΓΛΩΠΨΣΘΞ€"; 
	    	 String test1 = "s, your cpf money will be distributed to your spouse kids if any, if not your parents or your siblings. So by revoking your existing one alone, it shall中";
	    	 String test2 = "@$_^{}\\\\[~]|!\\\"#%&'()*+,-./0123456789:;<=>?ABCDEFGHIJKLMNOPQRS" +
	    	 		"TUVWXYZabcdefghijklmnopqrstuvwxyz£¥èéù中";
	    	System.out.println(test1.length());
	    	 byte t[] = Convert.convert2GSM(test1);
	    	 System.out.println(Arrays.toString(Convert.byte2int(t)));
			System.out.println(Arrays.toString(Convert.byte2int(Convert.encode7bit(t, 1))));			
			System.out.println(Convert.byte2int(Convert.encode7bit(t, 1)));			
			System.out.println(Arrays.toString(Convert.decodeGSM7Bit(Convert.encode7bit(t,1),1)));				
			System.out.println(Convert.convertGSM2String(Convert.decodeGSM7Bit(Convert.encode7bit(Convert.convert2GSM(test2),0),0)));	
			String a="中";
			try {
				System.out.println(a.getBytes("UnicodeBigUnmarked").length);
			} catch (Exception e) {
				// TODO: handle exception
			}
			
		}
}
