package com.atguigu.leetcode;

import java.util.HashMap;
import java.util.Map;
/*
给定一个整数数组 nums 和一个目标值 target，请你在该数组中找出和为目标值的那 两
个 整数，并返回他们的数组下标。
你可以假设每种输入只会对应一个答案。但是，你不能重复利用这个数组中同样的元素。

```
给定 nums = [2, 7, 11, 15], target = 9

因为 nums[0] + nums[1] = 2 + 7 = 9
所以返回 [0, 1]
 */
public class Solution {
    public static void main(String[] args) {
        int[] a = {2,11,7,37};
        int[] result = twoSum(a,9);
        System.out.println("[" + result[0] + "," + result[1] + "]");
    }
    //{2,7,11,15}  9
    public static int[] twoSum(int[] nums,int target){
        Map<Integer,Integer> map = new HashMap<>();
        for (int i = 0;i < nums.length;i++){
            int complete = target - nums[i];
            if (map.containsKey(complete)){
                return new int[] {map.get(complete),i};
            }
            map.put(nums[i],i);
        }
        throw new IllegalArgumentException("No two sum solution");
        //System.out.println("No two sum solution");
    }
}
