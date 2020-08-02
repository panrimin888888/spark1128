package util;

public class BuffleSort {
    public static void main(String[] args) {
        int[] arr = {1,2,5,9,3,80,-99,4};
        int temp = 0;
        boolean flag = false;
        for (int i = 0;i < arr.length - 1;i++){
            for (int j = 0;j < arr.length - i - 1;j++){
                if(arr[j] > arr[j+1]){
                    flag = true;
                    temp = arr[j];
                    arr[j] = arr[j+1];
                    arr[j+1] = temp;
                }
            }
            if(!flag){
                break;
            }else{
                flag = false;
            }
            System.out.printf("第%d次循环的结果是：" , i+1);
            for(int k : arr){
                System.out.print(k+" ");
            }
            System.out.println();
        }
    }
}
