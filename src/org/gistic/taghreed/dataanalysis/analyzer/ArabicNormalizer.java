package org.gistic.taghreed.dataanalysis.analyzer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;

/**
 * Arabic Normalizer - Ported from Perl code of Walid Magdy (QCRI)
 *
 * Paper: Kareem Darwish, Walid Magdy, Ahmed Mourad, "Language Processing for Arabic Microblog Retrieval", CIKM 2012
 * 
 * @author Amgad Madkour
 */

public class ArabicNormalizer {

    private static String dicFile = "resources/JSC.pruned.dic";
    private static String elongFile = "resources/Elong.class.txt";
    private static HashMap<String, String> dic;
    private static HashMap<String, String> norm;

    public ArabicNormalizer() {

        BufferedReader dicrdr = null;
        BufferedReader elongrdr = null;
        String temp = null;
        int mostFreq;
        Pattern pattern,pattern2;
        Matcher matcher2;
        String word,base;
        
        dic = new HashMap<String, String>();
        norm = new HashMap<String, String>();

        try {
            dicrdr = new BufferedReader(new FileReader(dicFile));
        } catch (FileNotFoundException ex) {
            System.out.println("Cant find file " + dicFile);
        }

        try {
            elongrdr = new BufferedReader(new FileReader(elongFile));
        } catch (FileNotFoundException ex) {
            System.out.println("Cant find file " + elongFile);
        }

        dic = new HashMap<String, String>();
        pattern = Pattern.compile("(\\S\\S+)\\s+(\\d\\d+)");
        Matcher matcher = null;

        try {
            while ((temp = dicrdr.readLine()) != null) {
                matcher = pattern.matcher(temp);
                if (matcher.find()) {
                    dic.put(matcher.group(1), matcher.group(2));
                }
            }

        } catch (IOException ex) {
            System.out.println("Cant read file " + dicFile);
        }

        mostFreq = 1;
        pattern = Pattern.compile("----------");
        pattern2 = Pattern.compile("(\\S+)\\s+\\d+");
        
        try {    
            while ((temp = elongrdr.readLine()) != null) {
                matcher = pattern.matcher(temp);
                
                if(matcher.find()){
                    mostFreq = 1;
                }else if(mostFreq == 1 && (matcher2 = pattern2.matcher(temp)).find()){
                    word = base = matcher2.group(1);
                    base = base.replaceAll("(\\S)\\1+", "$1"); //$base =~ s/(\S)\1+/\1/g;
                    norm.put(base, word);
                    mostFreq = 0;
                }
            }
        } catch (IOException ex) {
            Logger.getLogger(ArabicNormalizer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
	
    public String normalize(String text){
        
        String txt,finalTxt,shortTxt;
        
        txt = text;
        txt = txt.replaceAll("[ًٌٍَُِّـْ]", "");
        txt = StringUtils.replaceChars(txt,"أآإىة","ااايه");
        txt = txt.replaceAll("[ﻻﻵﻷעﻹﻼﻶﻸﬠ]","لا");
        txt = StringUtils.replaceChars(txt, "٠١٢۲٣۳٤٥Ƽ٦٧۷٨۸٩۹ﺀٴٱﺂﭑﺎﺈﺄιٲﺍٳίﺃٵﺇﺁﺑپﮨﺒٻﺐﺏﭘﭒﭗﭖﭚٮﭛٺﺗﭠﺘټﺖﺕﭡٹﭞٿﭟﭤﮢﭥﭨﭢﭣﮣﭧﺛﺜﮆﺚﺙٽﮇچﺟﭴﺠﭼڄڇﭸﺝڃﺞﭽﮀﭵﭹﭻﭾﭿﭺﺣﺤﺡﺢځﺧﺨڅڂﺦﺥڿډﺩڍﺪڊڈﮃﮂڋﮈڌﮉڐﮄﺫﺬڎڏۮڕړﺮﺭڒڔږڑژﮌڗﮍڙﺯﺰﮊﺳڛﺴﺲﺱښﺷڜﺸﺶﺵۺﺻﺼڝﺺﺹﺿﻀﺽڞﺾۻﻃﻁﻄﻂﻈﻇﻅڟﻆﻋ۶ﻌﻊﻉﻏﻐڠۼﻍﻎﻓڤﻔﭬڣﭰﻒﻑڦڢڡﭫڥﭪﭭﭯﭮﻗﻘڨﻖﻕڧﭱگڳکڪڱﮔﻛﮘڰﮐﮖﻜﮜڲﻚڴﮗڭﻙﮓﮙګڮﮕﮛڬﮎﮝﮚﮑﮒﮏﯖﯕﻟڵڷﻠڶﻞﻝڸﻣﻤﻢﻡﻧﻥڼﻨﻦڻڽﮠڹﮞںטּﮡﮟھہۃﮬﮪﮧۂﻫﮫﺔﻪﻬﮭﺓۿﻩەۀﮤﮥﮦۆۈۅﯙۉﻭﻮۄۋۇۊﯚٷٶﯛﯠﺆﯜۏﺅﯡﯝﯘﯢﯞﯣﯗﯟﯾےﻳۓېێﮱﻴﮯﭔﻲۑۍﯿﻱﻰﭜڀﺋﻯﭕﮮﺌﭓﯼﭝ༦ﺊﯽﮰﭙﯥﺉﯦﯧﯤیٸ","0122334556778899ءءاااااااااااااااببببببببببببببتتتتتتتتتتتتتتتتتتتتثثثثثثثجججججججججججججججججججحححححخخخخخخخددددددددددددددذذذذذرررررررررررررزززسسسسسسششششششصصصصصضضضضضضططططظظظظظعععععغغغغغغفففففففففففففففففقققققققككككككككككككككككككككككككككككككككككللللللللممممننننننننننننننهههههههههههههههههههههوووووووووووووووووووووووووووويييييييييييييييييييييييييييييييييييييي");
        txt = txt.replaceAll("http\\S*", ""); // removing links
        txt = txt.replaceAll("\\@\\S+","");
        txt = txt.replaceAll("RT ",""); //removing name mentions 
        txt = txt.replaceAll("\\:\\-*[\\)D]+"," ");
        txt = Pattern.compile(" l+o+l+ ", Pattern.CASE_INSENSITIVE).matcher(txt).replaceAll(" ");
        txt = txt.replaceAll(" ل+و+ل+ ", " ");
        txt = txt.toLowerCase();
        txt = txt.replaceAll("[^اأإآبتثجحخدذرزسشصضطظعغفقكلمنهويىئءؤةa-z0-9\\@\\#\\_\\s]+", " ");

        finalTxt = "";
        shortTxt = "";
    
        Pattern pattern = Pattern.compile("(\\S+)");
        Matcher matcher = pattern.matcher(txt);
        
        while(matcher.find()){
            shortTxt = shorten(matcher.group(1));
            finalTxt += shortTxt + " ";
        }
        
        finalTxt = finalTxt.trim();

        return finalTxt;
    }
    
    public String shorten(String val){
        
        String word,base,normTxt;
        
        word = base = val;
        
        Pattern pattern = Pattern.compile("[^ابتثجحخدذرزسشصضطظعغفقكلمنهويآإءىةئؤ]");
        Matcher matcher = pattern.matcher(word);
        
        if(matcher.find()){
            return word;
        }else{
            if(dic.containsKey(word)){
                normTxt = word;
            }else{
                base = base.replaceAll("(\\S)\\1+", "$1");
                
                if(norm.containsKey(base)){
                    normTxt = norm.get(base);
                }else{
                    normTxt = base; //Inv
                }
            }
            return normTxt;
        }
    }
}
