HI,

Now i am writing java code for reduce side join with 3 input files, which i place 1 file in distributed cache to join.

Step1:

In DeliverFileMapper.java file i am processing the UserDetails.txt file

Step2:

In DeliverFileMapper.java file i am processing the DeliveryDetails.txt file

Step3:

In SmsReducer.java file i am joining the two files that processesd from the above 2 mappers and i placed DeliveryStatusCodes.txt in distributed cache as this is small file.Now i process all the 3 files to join and will get the below desired results.

  Expected Output
Jim, Delivered
Tom, Pending
Harry, Failed
Richa, Resend

   Sample Inputs    

    File 1 – UserDetails.txt
123456, Jim
456123, Tom
789123, Harry
789456, Richa

    File 2 – DeliveryDetails.txt
123456, 001
456123, 002
789123, 003
789456, 004

    File 3 – DeliveryStatusCodes.txt
001, Delivered
002, Pending
003, Failed
004, Resend

    Expected Output
Jim, Delivered
Tom, Pending
Harry, Failed
Richa, Resend
