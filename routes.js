const express = require("express")
const Post = require("./model/post")
const router = express.Router();
const auth=require("./middleware/auth")
const { check, validationResult} = require("express-validator");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");

const User = require("./model/user");

const SECRET_KEY = "sk_test_51MSFMaSIDUaT83RH4vnhsd912OjOTu0H24F7JeBJocmKGdFZSonVLag0uxO25rPnAnICc1jNkNP2Eb9Io2U4bSHz00hQv8daDj";
const stripe = require("stripe")(SECRET_KEY);
const logger = require('./logger')
const { Client } = require('@opensearch-project/opensearch');

const openSearchClient = new Client({ node: 'http://localhost:9200' });

router.get("/blog", async (req, res) => {
  try {
    // Extract query parameters
    const { title, content, address, first_name, search, sortBy, sortOrder, page, limit } = req.query;

    // MongoDB filtering based on specific fields
    let filter = {};

    // Support search across multiple fields (title, content, address, first_name)
    if (search) {
      filter.$or = [
        { title: { $regex: search, $options: "i" } },
        { content: { $regex: search, $options: "i" } },
        { address: { $regex: search, $options: "i" } },
        { first_name: { $regex: search, $options: "i" } },
      ];
    }

    // Apply individual field filters if provided
    if (title) filter.title = { $regex: title, $options: "i" };
    if (content) filter.content = { $regex: content, $options: "i" };
    if (address) filter.address = { $regex: address, $options: "i" };
    if (first_name) filter.first_name = { $regex: first_name, $options: "i" };

    // Set default pagination values
    const pageNumber = parseInt(page) || 1;
    const pageSize = parseInt(limit) || 10;
    const skip = (pageNumber - 1) * pageSize;

    // Set sorting options (default: sort by creation date in descending order)
    const sortField = sortBy || 'createdAt';  // Adjust field name as needed
    const sortDirection = sortOrder === 'asc' ? 1 : -1;

    // Query MongoDB with filters, sorting, and pagination
    const posts = await Post.find(filter)
      .sort({ [sortField]: sortDirection })
      .skip(skip)
      .limit(pageSize);

    // Get total document count for pagination
    const totalDocuments = await Post.countDocuments(filter);

    // Send response with MongoDB data
    res.send({
      status: 200,
      data: posts,
      total: totalDocuments,  // Total matching posts
      page: pageNumber,
      limit: pageSize,
      success: true,
    });
  } catch (error) {
    console.error('Error fetching posts: ', error);  // Log the detailed error
    res.status(500).send({
      status: 500,
      message: "Error fetching posts",
      success: false,
      error: error.message,  // Add error message in the response
    });
  }
});

// router.get("/blog", async (req, res) => {
//   try {
//     // Extract query parameters
//     const { title, content, address, first_name, sortBy, sortOrder, page, limit } = req.query;

//     // MongoDB filtering based on specific fields
//     let filter = {}; 
//     if (title) filter.title = { $regex: title, $options: "i" };
//     if (content) filter.content = { $regex: content, $options: "i" };
//     if (address) filter.address = { $regex: address, $options: "i" };
//     if (first_name) filter.first_name = { $regex: first_name, $options: "i" };

//     // Set default pagination values
//     const pageNumber = parseInt(page) || 1;
//     const pageSize = parseInt(limit) || 10;
//     const skip = (pageNumber - 1) * pageSize;

//     // Set sorting options (default: sort by creation date in descending order)
//     const sortField = sortBy || 'createdAt';  // Adjust field name as needed
//     const sortDirection = sortOrder === 'asc' ? 1 : -1;

//     // Query MongoDB with filters, sorting, and pagination
//     const posts = await Post.find(filter)
//       .sort({ [sortField]: sortDirection })
//       .skip(skip)
//       .limit(pageSize);

//     // Get total document count for pagination
//     const totalDocuments = await Post.countDocuments(filter);

//     // Send response with MongoDB data
//     res.send({
//       status: 200,
//       data: posts,
//       total: totalDocuments,  // Total matching posts
//       page: pageNumber,
//       limit: pageSize,
//       success: true,
//     });
//   } catch (error) {
//     console.error('Error fetching posts: ', error);  // Log the detailed error
//     res.status(500).send({
//       status: 500,
//       message: "Error fetching posts",
//       success: false,
//       error: error.message,  // Add error message in the response
//     });
//   }
// });


router.post("/blog", async (req, res) => {
  try {
    const post = new Post({
      title: req.body.title,
      content: req.body.content,
      address: req.body.address,
      first_name: req.body.first_name,
    });
    
    await post.save();
    
    res.status(200).send({
      message: "Record saved successfully.",
      success: true,
    });
  } catch (error) {
    logger.error('Error while saving new data: ', error);
    res.status(500).send({
      message: "Internal server error.",
      success: false,
      error: error.message 
    });
  }
});


router.get("/blog/:id", async (req, res) => {
	try {
		const post = await Post.findOne({ _id: req.params.id })
		// res.send(post)
    res.send({
      status: 200 , data: post,
      message: "Record Fetched Successfully",
      success: true
    })
    
	} catch {
		res.status(404)
    res.send({ status: 404 ,message: "Record Not Found",
    success: false })
	}
})

router.patch("/blog/:id", async (req, res) => {
	try {
		const post = await Post.findOne({ _id: req.params.id })

		if (req.body.title) {
			post.title = req.body.title
		}

		if (req.body.content) {
			post.content = req.body.content
		}
		await post.save()
    res.send({
      status: 200 , message: "Successfully updated Record",
      success: true
    })
	} catch {
		res.status(404)
		res.send({ status: 404 ,message: "Record doesn't exist!",
      success: false })
	}
})


router.delete("/blog/:id", async (req, res) => {
	try {
		await Post.deleteOne({ _id: req.params.id })
		res.status(200)
    res.send({
      status: 200 ,message: "Record Deleted Successfully",
      success: true
    })
	} catch {
		res.status(404)
    res.send({ status: 404 ,message: "Record doesn't exist!",
      success: false })
	}
});

router.post(
  "/signup",
  [
      check("email", "Please enter a valid email").isEmail(),
      check("password", "Please enter a valid password").isLength({
          min: 6
      })
  ],
  async (req, res) => {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        // return res.status(400).json({
        //     errors: errors.array()
        // });
        return res.send({ status: 400 ,message: errors,
          success: false });
      }
      const { email, password } = req.body;
      try {
        let user = await User.findOne({email});
        if (user) {
        return res.send({ status: 400 ,message: "User Already Exists",
          success: false });
        }
        user = new User({
            email,
            password
        });

        const salt = await bcrypt.genSalt(10);
        user.password = await bcrypt.hash(password, salt);

        await user.save();
        const payload = { user: { id: user.id} };

        jwt.sign(
          payload,
          "randomString", {
              expiresIn: 10000
          },
          (err, token) => {
            if (err) throw err;
            res.status(200).json({
              token
            });
          }
        );
      } catch (err) {
        console.log(err.message);
        // res.status(500).send("Error in Saving");
        res.send({
          status: 500 ,message: "Error in Saving",
          success: false
        })
      }
  }
);


router.post(
  "/login",
  [
    check("email", "Please enter a valid email").isEmail(),
    check("password", "Please enter a valid password").isLength({
      min: 6
    })
  ],
  async (req, res) => {
    const errors = validationResult(req);

    if (!errors.isEmpty()) {
      return res.status(400).json({
        errors: errors.array()
      });
    }

    const { email, password } = req.body;
    try {
      let user = await User.findOne({
        email
      });
      if (!user)
        // return res.status(400).json({
        //   message: "User Not Exist"
        // });
        return res.send({ status: 400, message: "User Not Exist", success: false });
  
      const isMatch = await bcrypt.compare(password, user.password);
      if (!isMatch)
        return res.send({ status: 400, 
          message: "Incorrect Password !", success: false });

      const payload = {
        user: {
          id: user.id
        }
      };

      jwt.sign(
        payload,
        "randomString",
        {
          expiresIn: 3600
        },
        (err, token) => {
          if (err) throw err;
          res.status(200).json({
            token
          });
        }
      );
    } catch (e) {
      return res.send({ status: 500, 
        message: "Server Error", success: false });
    }
  }
);


router.get("/me", auth, async (req, res) => {
  try {
    // request.user is getting fetched from Middleware after token authentication
    const user = await User.findById(req.user.id);
    res.json(user);
    logger.info('correct token');
  } catch (e) {
    res.send({ message: "Error in Fetching user" });
  }
});


// router.post("/api/payment", async (req, res) => {
//   stripe.customers.create({
//     email: req.body.stripeEmail,
//     source: req.body.stripeToken,
//     name: 'abc Sharma',
//     address: {
//     line1: 'TC 9/4 Old MES colony',
//     postal_code: '110092',
//     city: 'New Delhi',
//     state: 'Delhi',
//     country: 'India',
//     }
//     })
//     .then((customer) => {
     
//     return stripe.charges.create({
//     amount: 7000, // Charing Rs 25
//     description: 'Web Development Product',
//     currency: 'USD',
//     customer: customer.id
//     });
//     })
//     .then((charge) => {
//       res.send("Success") // If no error occurs
//     })
//     .catch((err) => {
//       res.send(err) // If some error occurs
//     });
// });

module.exports = router