// package com.atguigu.leetcode;
//
//
// import javax.swing.tree.TreeNode;
//
// public class Solution2 {
//     public static void main(String[] args) {
//
//     }
//
//     public static TreeNode invertTree(TreeNode root){
//         if (root == null){
//             return null;
//         }
//         TreeNode right = invertTree(root.right);
//         TreeNode left = invertTree(root.left);
//         root.left = right;
//         root.right = left;
//         return root;
//     }
// }
