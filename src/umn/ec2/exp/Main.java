package umn.ec2.exp;

public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if(args.length ==2){
			String operation = args[0];
			String level = args[1];
		}else{
			System.out.println("To use this program you must pass the following arguments\n*********\n"
					+ "index [level(day,week,month)]\n"
					+ "query [level(day,week,month)]\n"
					+ "query temporal\t this is will query from all levels");
		}
	}

}
